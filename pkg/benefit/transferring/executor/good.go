package executor

import (
	"context"
	"encoding/json"
	"fmt"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/transferring/types"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent            chan interface{}
	notif                 chan interface{}
	done                  chan interface{}
	coin                  *coinmwpb.Coin
	newBenefitState       goodtypes.BenefitState
	toPlatformAmount      decimal.Decimal
	techniqueFee          decimal.Decimal
	nextStartRewardAmount decimal.Decimal
	transferToPlatform    bool
	userBenefitHotAccount *pltfaccmwpb.Account
	platformColdAccount   *pltfaccmwpb.Account
	benefitResult         basetypes.Result
	benefitMessage        string
	txExtra               string
}

const (
	errorInvalidTx = "Invalid transaction"
	errorTxFail    = "Transaction fail"
)

func (h *goodHandler) getCoin(ctx context.Context) error {
	coin, err := coinmwcli.GetCoin(ctx, h.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.coin = coin
	return nil
}

func (h *goodHandler) checkTransfer(ctx context.Context) error {
	tx, err := txmwcli.GetTx(ctx, h.RewardTID)
	if err != nil {
		return err
	}
	if tx == nil {
		h.benefitResult = basetypes.Result_Fail
		h.benefitMessage = fmt.Sprintf("%v (%v)", errorInvalidTx, h.RewardTID)
		h.newBenefitState = goodtypes.BenefitState_BenefitFail
		return nil
	}

	switch tx.State {
	case basetypes.TxState_TxStateCreated:
		fallthrough //nolint
	case basetypes.TxState_TxStateWait:
		fallthrough //nolint
	case basetypes.TxState_TxStateTransferring:
		return nil
	case basetypes.TxState_TxStateFail:
		h.benefitResult = basetypes.Result_Fail
		h.benefitMessage = fmt.Sprintf("%v (%v)", errorTxFail, h.RewardTID)
		h.newBenefitState = goodtypes.BenefitState_BenefitFail
	case basetypes.TxState_TxStateSuccessful:
		h.newBenefitState = goodtypes.BenefitState_BenefitBookKeeping
	}

	if h.newBenefitState == goodtypes.BenefitState_BenefitBookKeeping {
		type p struct {
			PlatformReward      decimal.Decimal
			TechniqueServiceFee decimal.Decimal
			GoodID              string
		}
		_p := p{}
		err = json.Unmarshal([]byte(tx.Extra), &_p)
		if err != nil {
			return err
		}

		h.toPlatformAmount = _p.PlatformReward.Add(_p.TechniqueServiceFee)
		h.techniqueFee = _p.TechniqueServiceFee
	}

	nextStart, err := decimal.NewFromString(h.NextRewardStartAmount)
	if err != nil {
		return err
	}
	amount, err := decimal.NewFromString(tx.Amount)
	if err != nil {
		return err
	}
	h.nextStartRewardAmount = nextStart.Sub(amount)
	h.txExtra = tx.Extra

	return nil
}

func (h *goodHandler) checkTransferToPlatform() error {
	least, err := decimal.NewFromString(h.coin.LeastTransferAmount)
	if err != nil {
		return err
	}
	if least.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid leasttransferamount")
	}

	if h.toPlatformAmount.Cmp(least) <= 0 {
		return nil
	}
	h.transferToPlatform = true
	return nil
}

func (h *goodHandler) getPlatformAccount(ctx context.Context, usedFor basetypes.AccountUsedFor) (*pltfaccmwpb.Account, error) {
	account, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(usedFor)},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("invalid account")
	}
	return account, nil
}

//nolint:gocritic
func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Good", h.Good,
			"ToPlatformAmount", h.toPlatformAmount,
			"NewBenefitState", h.newBenefitState,
			"NextStartAmount", h.nextStartRewardAmount,
			"Extra", h.txExtra,
			"UserBenefitHotAccount", h.userBenefitHotAccount,
			"PlatformColdAccount", h.platformColdAccount,
			"BenefitResult", h.benefitResult,
			"BenefitMessage", h.benefitMessage,
			"Error", *err,
		)
	}
	persistentGood := &types.PersistentGood{
		Good:               h.Good,
		TransferToPlatform: h.transferToPlatform,
		ToPlatformAmount:   h.toPlatformAmount.String(),
		NewBenefitState:    h.newBenefitState,
		FeeAmount:          decimal.NewFromInt(0).String(),
		NextStartAmount:    h.nextStartRewardAmount.String(),
		Extra:              h.txExtra,
		Error:              *err,
	}
	if h.userBenefitHotAccount != nil {
		persistentGood.UserBenefitHotAccountID = h.userBenefitHotAccount.AccountID
		persistentGood.UserBenefitHotAddress = h.userBenefitHotAccount.Address
	}
	if h.platformColdAccount != nil {
		persistentGood.PlatformColdAccountID = h.platformColdAccount.AccountID
		persistentGood.PlatformColdAddress = h.platformColdAccount.Address
	}

	if h.newBenefitState == h.RewardState && *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
		return
	}
	if h.newBenefitState == goodtypes.BenefitState_BenefitFail {
		persistentGood.BenefitResult = h.benefitResult
		persistentGood.BenefitMessage = h.benefitMessage
	}
	if *err != nil {
		persistentGood.BenefitResult = basetypes.Result_Fail
		persistentGood.BenefitMessage = (*err).Error()
		asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	}
	if h.newBenefitState != h.RewardState {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	h.newBenefitState = h.RewardState
	var err error
	defer h.final(ctx, &err)

	if err = h.checkTransfer(ctx); err != nil {
		return err
	}
	if h.newBenefitState == goodtypes.BenefitState_BenefitFail {
		return nil
	}
	if err = h.getCoin(ctx); err != nil {
		return err
	}
	if err = h.checkTransferToPlatform(); err != nil {
		return err
	}
	if !h.transferToPlatform {
		return nil
	}
	h.userBenefitHotAccount, err = h.getPlatformAccount(ctx, basetypes.AccountUsedFor_UserBenefitHot)
	if err != nil {
		return err
	}
	h.platformColdAccount, err = h.getPlatformAccount(ctx, basetypes.AccountUsedFor_PlatformBenefitCold)
	if err != nil {
		return err
	}

	return nil
}
