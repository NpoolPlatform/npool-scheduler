package executor

import (
	"context"
	"fmt"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	appcoinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/app/coin"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	reviewtypes "github.com/NpoolPlatform/message/npool/basetypes/review/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appcoinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/app/coin"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/reviewing/types"
	reviewmwcli "github.com/NpoolPlatform/review-middleware/pkg/client/review"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent                chan interface{}
	notif                     chan interface{}
	withdrawAmount            decimal.Decimal
	newWithdrawState          ledgertypes.WithdrawState
	newReviewState            reviewtypes.ReviewState
	userBenefitHotAccount     *pltfaccmwpb.Account
	userBenefitHotBalance     decimal.Decimal
	userBenefitHotFeeBalance  decimal.Decimal
	appCoin                   *appcoinmwpb.Coin
	feeCoin                   *coinmwpb.Coin
	autoReviewThresholdAmount decimal.Decimal
	coinReservedAmount        decimal.Decimal
	lowFeeAmount              decimal.Decimal
}

func (h *withdrawHandler) checkWithdrawReview(ctx context.Context) error {
	review, err := reviewmwcli.GetReview(ctx, h.ReviewID)
	if err != nil {
		return err
	}
	switch review.State {
	case reviewtypes.ReviewState_Approved:
		h.newWithdrawState = ledgertypes.WithdrawState_Approved
	case reviewtypes.ReviewState_Rejected:
		h.newWithdrawState = ledgertypes.WithdrawState_PreRejected
	}
	return nil
}

func (h *withdrawHandler) getAppCoin(ctx context.Context) error {
	coin, err := appcoinmwcli.GetCoinOnly(ctx, &appcoinmwpb.Conds{
		AppID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		Disabled:   &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.appCoin = coin
	return nil
}

func (h *withdrawHandler) getFeeCoin(ctx context.Context) error {
	coin, err := coinmwcli.GetCoin(ctx, h.appCoin.FeeCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.feeCoin = coin
	return nil
}

func (h *withdrawHandler) getUserBenefitHotAccount(ctx context.Context) error {
	account, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountUsedFor_UserBenefitHot)},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid account")
	}
	h.userBenefitHotAccount = account
	return nil
}

func (h *withdrawHandler) checkUserBenefitHotBalance(ctx context.Context) error {
	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.appCoin.Name,
		Address: h.userBenefitHotAccount.Address,
	})
	if err != nil {
		return err
	}
	if bal == nil {
		return fmt.Errorf("invalid balance")
	}
	h.userBenefitHotBalance, err = decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}
	return nil
}

func (h *withdrawHandler) checkUserBenefitHotFeeBalance(ctx context.Context) error {
	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.appCoin.FeeCoinName,
		Address: h.userBenefitHotAccount.Address,
	})
	if err != nil {
		return err
	}
	if bal == nil {
		return fmt.Errorf("invalid fee balance")
	}
	h.userBenefitHotFeeBalance, err = decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}
	return nil
}

func (h *withdrawHandler) checkWithdrawReviewState() error {
	if h.userBenefitHotBalance.Cmp(h.withdrawAmount.Add(h.coinReservedAmount)) <= 0 {
		return fmt.Errorf("insufficient funds")
	}
	if h.userBenefitHotFeeBalance.Cmp(h.lowFeeAmount) < 0 {
		return fmt.Errorf("insufficient gas")
	}
	if h.autoReviewThresholdAmount.Cmp(h.withdrawAmount) < 0 {
		return nil
	}
	h.newWithdrawState = ledgertypes.WithdrawState_Approved
	h.newReviewState = reviewtypes.ReviewState_Approved
	return nil
}

func (h *withdrawHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Withdraw", h.Withdraw,
			"NewWithdrawState", h.newWithdrawState,
			"NewReviewState", h.newReviewState,
			"Error", *err,
		)
	}
	if h.newWithdrawState == h.State && *err == nil {
		return
	}

	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw:         h.Withdraw,
		NewWithdrawState: h.newWithdrawState,
		NewReviewState:   h.newReviewState,
		Error:            *err,
	}

	if *err == nil {
		h.persistent <- persistentWithdraw
	} else {
		h.notif <- persistentWithdraw
	}
}

func (h *withdrawHandler) exec(ctx context.Context) error {
	h.newWithdrawState = h.State

	var err error
	defer h.final(ctx, &err)

	if err = h.checkWithdrawReview(ctx); err != nil {
		return err
	}
	h.withdrawAmount, err = decimal.NewFromString(h.Amount)
	if err != nil {
		return err
	}
	if err = h.getAppCoin(ctx); err != nil {
		return err
	}
	if err = h.getFeeCoin(ctx); err != nil {
		return err
	}
	h.autoReviewThresholdAmount, err = decimal.NewFromString(h.appCoin.WithdrawAutoReviewAmount)
	if err != nil {
		return err
	}
	h.coinReservedAmount, err = decimal.NewFromString(h.appCoin.ReservedAmount)
	if err != nil {
		return err
	}
	h.lowFeeAmount, err = decimal.NewFromString(h.feeCoin.LowFeeAmount)
	if err != nil {
		return err
	}
	if err = h.getUserBenefitHotAccount(ctx); err != nil {
		return err
	}
	if err = h.checkUserBenefitHotBalance(ctx); err != nil {
		return err
	}
	if err = h.checkUserBenefitHotFeeBalance(ctx); err != nil {
		return err
	}
	if err = h.checkWithdrawReviewState(); err != nil {
		return err
	}

	return nil
}
