package executor

import (
	"context"
	"fmt"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	useraccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/user"
	appcoinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/app/coin"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	currencymwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	useraccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/user"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appcoinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/app/coin"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	currencymwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/approved/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent                chan interface{}
	notif                     chan interface{}
	done                      chan interface{}
	withdrawAmount            decimal.Decimal
	feeAmount                 decimal.Decimal
	newWithdrawState          ledgertypes.WithdrawState
	withdrawAccount           *useraccmwpb.Account
	userBenefitHotAccount     *pltfaccmwpb.Account
	userBenefitHotBalance     decimal.Decimal
	userBenefitHotFeeBalance  decimal.Decimal
	appCoin                   *appcoinmwpb.Coin
	feeCoin                   *coinmwpb.Coin
	autoReviewThresholdAmount decimal.Decimal
	coinReservedAmount        decimal.Decimal
	lowFeeAmount              decimal.Decimal
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
	h.newWithdrawState = ledgertypes.WithdrawState_Transferring
	return nil
}

func (h *withdrawHandler) getWithdrawAccount(ctx context.Context) error {
	account, err := useraccmwcli.GetAccountOnly(ctx, &useraccmwpb.Conds{
		AppID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		UserID:     &basetypes.StringVal{Op: cruder.EQ, Value: h.UserID},
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		AccountID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.AccountID},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountUsedFor_UserWithdraw)},
	})
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid account")
	}
	h.withdrawAccount = account
	return nil
}

func (h *withdrawHandler) calculateFeeAmount(ctx context.Context) error {
	amount, err := decimal.NewFromString(h.appCoin.WithdrawFeeAmount)
	if err != nil {
		return err
	}
	if amount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid fee amount")
	}
	if !h.appCoin.WithdrawFeeByStableUSD {
		h.feeAmount = amount
		return nil
	}
	curr, err := currencymwcli.GetCurrencyOnly(ctx, &currencymwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.appCoin.CoinTypeID},
	})
	if err != nil {
		return err
	}
	value, err := decimal.NewFromString(curr.MarketValueLow)
	if err != nil {
		return err
	}
	if value.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid coin price")
	}
	h.feeAmount = amount.Div(value)
	return nil
}

func (h *withdrawHandler) validateFeeAmount() error {
	if h.withdrawAmount.Cmp(h.feeAmount) <= 0 {
		return fmt.Errorf("invalid amount")
	}
	return nil
}

//nolint:gocritic
func (h *withdrawHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Withdraw", h.Withdraw,
			"Error", *err,
		)
	}
	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw:          h.Withdraw,
		NewWithdrawState:  h.newWithdrawState,
		WithdrawAmount:    h.Amount,
		WithdrawFeeAmount: h.feeAmount.String(),
		Error:             *err,
	}
	if h.userBenefitHotAccount != nil && h.withdrawAccount != nil {
		persistentWithdraw.UserBenefitHotAccountID = h.userBenefitHotAccount.AccountID
		persistentWithdraw.UserBenefitHotAddress = h.userBenefitHotAccount.Address
		withdrawExtra := fmt.Sprintf(
			`{"AppID":"%v","UserID":"%v","Address":"%v","CoinName":"%v","WithdrawID":"%v"}`,
			h.AppID,
			h.UserID,
			h.withdrawAccount.Address,
			h.appCoin.Name,
			h.ID,
		)
		persistentWithdraw.WithdrawExtra = withdrawExtra
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.persistent)
		return
	} else {
		asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.notif)
		asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.done)
	}
}

//nolint:gocritic
func (h *withdrawHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

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
	if err = h.getWithdrawAccount(ctx); err != nil {
		return err
	}
	if err = h.checkWithdrawReviewState(); err != nil {
		return err
	}
	if err = h.calculateFeeAmount(ctx); err != nil {
		return err
	}
	if err = h.validateFeeAmount(); err != nil {
		return err
	}

	return nil
}
