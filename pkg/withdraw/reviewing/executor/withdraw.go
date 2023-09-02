package executor

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	reviewmwpb "github.com/NpoolPlatform/message/npool/review/mw/v2/review"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/transferring/types"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent                chan interface{}
	notif                     chan interface{}
	withdrawAmount            decimal.Decimal
	newWithdrawState          ledgertypes.WithdrawState
	reviewTrigger             reviewmwpb.ReviewTriggerType
	userBenefitHotAccount     *pltfaccmwpb.Account
	userBenefitHotBalance     decimal.Decimal
	userBenefitHotFeeBalance  decimal.Decimal
	review                    *reviewmwpb.Review
	reviewReq                 *reviewmwpb.ReviewReq
	appCoin                   *appcoinmwpb.Coin
	feeCoin                   *coinmwpb.Coin
	autoReviewThresholdAmount decimal.Decimal
	coinReservedAmount        decimal.Decimal
	lowFeeAmount              decimal.Decimal
}

func (h *withdrawHandler) getAppCoin(ctx context.Context) error {
	coin, err := appcoinmwcli.GetCoinOnly(ctx, &appcoinmwpb.Conds{
		AppID:      &basetypes.StringVal{Op: cruder.EQ, Value: appID},
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: coinTypeID},
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

func (h *withdrawHandler) checkWithdrawReview(ctx context.Context) error {
	review, err := reviewmwcli.GetReview(ctx, h.ReviewID)
	if err != nil {
		return err
	}
	h.review = review
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
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("invalid account")
	}
	h.userBenefitHotAccount = account
	return nil
}

func (h *withdrawHandler) checkUserBenefitHotBalance(ctx context.Context) error {
	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.coin.Name,
		Address: h.userBenefitHotAccount.Address,
	})
	if err != nil {
		return nil, err
	}
	if bal == nil {
		return nil, fmt.Errorf("invalid balance")
	}
	h.userBenefitHotBalance, err = decimal.RequireFromString(bal.BalanceStr)
	if err != nil {
		return err
	}
	return nil
}

func (h *withdrawHandler) checkUserBenefitHotFeeBalance(ctx context.Context) error {
	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.coin.FeeCoinName,
		Address: h.userBenefitHotAccount.Address,
	})
	if err != nil {
		return nil, err
	}
	if bal == nil {
		return nil, fmt.Errorf("invalid fee balance")
	}
	balance, err := decimal.RequireFromString(bal.BalanceStr)
	if err != nil {
		return err
	}
	h.userBenefitHotFeeBalance, err = decimal.RequireFromString(bal.BalanceStr)
	if err != nil {
		return err
	}
	return nil
}

func (h *withdrawHandler) resolveReviewTrigger() {
	if h.review != nil {
		return
	}
	h.reviewTrigger = reviewmwpb.ReviewTriggerType_AutoReviewed
	if h.userBenefitHotBalance.Cmp(h.withdrawAmount.Add(h.coinReservedAmount)) <= 0 {
		h.reviewTrigger = reviewmwpb.ReviewTriggerType_InsufficientFunds
	}
	if h.userBenefitHotFeeBalance.Cmp(h.lowFeeAmount) < 0 {
		switch h.reviewTrigger {
		case reviewpb.ReviewTriggerType_InsufficientFunds:
			h.reviewTrigger = reviewpb.ReviewTriggerType_InsufficientFundsGas
		case reviewpb.ReviewTriggerType_AutoReviewed:
			h.reviewTrigger = reviewpb.ReviewTriggerType_InsufficientGas
		}
		return
	}
	if h.autoReviewThresholdAmount(h.withdrawAmount) < 0 {
		h.reviewTrigger = reviewmwpb.ReviewTriggerType_LargeAmount
	}
}

func (h *withdrawHandler) resolveWithdrawAndReviewState() {
	if h.review != nil {
		switch h.review.State {
		case reviewmwpb.ReviewState_Rejected:
			h.newWithdrawState = ledgertypes.WithdrawState_Rejected
		case reviewmwpb.ReviewState_Approved:
			h.newWithdrawState = ledgertypes.WithdrawState_Transferring
		}
		return
	}
}

func (h *withdrawHandler) final(ctx context.Context, err *error) {
	if h.newWithdrwaState == h.State && *err == nil {
		return
	}

	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw:  h.Withdraw,
		NewState:  h.newState,
		ChainTxID: h.chainTxID,
		Error:     *err,
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
	if err = h.checkWithdrawReview(ctx); err != nil {
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
	h.resolveReviewTrigger()
	h.resolveWithdrawAndReviewState()

	return nil
}
