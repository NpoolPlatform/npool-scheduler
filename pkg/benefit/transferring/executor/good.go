package executor

import (
	"context"
	"encoding/json"
	"fmt"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/transferring/types"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent            chan interface{}
	notif                 chan interface{}
	retry                 chan interface{}
	coin                  *coinmwpb.Coin
	newBenefitState       goodtypes.BenefitState
	toPlatformAmount      decimal.Decimal
	techniqueFee          decimal.Decimal
	nextStartRewardAmount decimal.Decimal
	transferToPlatform    bool
	userBenefitHotAccount *pltfaccmwpb.Account
	platformColdAccount   *pltfaccmwpb.Account
	txExtra               string
}

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

func (h *goodHandler) checkTransferToPlatform(ctx context.Context) error {
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

func (h *goodHandler) final(ctx context.Context, err *error) {
	if h.newBenefitState == h.RewardState && *err == nil {
		return
	}

	persistentGood := &types.PersistentGood{
		Good:                    h.Good,
		TransferToPlatform:      h.transferToPlatform,
		ToPlatformAmount:        h.toPlatformAmount.String(),
		NewBenefitState:         h.newBenefitState,
		UserBenefitHotAccountID: h.userBenefitHotAccount.AccountID,
		UserBenefitHotAddress:   h.userBenefitHotAccount.Address,
		PlatformColdAccountID:   h.platformColdAccount.AccountID,
		PlatformColdAddress:     h.platformColdAccount.Address,
		FeeAmount:               decimal.NewFromInt(0).String(),
		NextStartAmount:         h.nextStartRewardAmount.String(),
		Extra:                   h.txExtra,
		Error:                   *err,
	}

	if *err == nil {
		h.persistent <- persistentGood
	} else {
		retry1.Retry(ctx, h.Good, h.retry)
		h.notif <- persistentGood
	}
}

func (h *goodHandler) exec(ctx context.Context) error {
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
	if err = h.checkTransferToPlatform(ctx); err != nil {
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
