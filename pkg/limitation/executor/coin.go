package executor

import (
	"context"
	"fmt"
	"time"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type coinHandler struct {
	*coinmwpb.Coin
	persistent             chan interface{}
	notif                  chan interface{}
	userBenefitHotAccount  *pltfaccmwpb.Account
	userBenefitColdAccount *pltfaccmwpb.Account
	amount                 decimal.Decimal
}

func (h *coinHandler) getPlatformAccount(ctx context.Context, usedFor basetypes.AccountUsedFor) (*pltfaccmwpb.Account, error) {
	account, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(usedFor)},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("invalid account")
	}
	return account, nil
}

func (h *coinHandler) checkBalanceLimitation(ctx context.Context) (bool, error) {
	limit, err := decimal.NewFromString(h.HotWalletAccountAmount)
	if err != nil {
		return false, err
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.Name,
		Address: h.userBenefitHotAccount.Address,
	})
	if err != nil {
		return false, err
	}
	if balance == nil {
		return false, fmt.Errorf("invalid balance")
	}

	bal, err := decimal.NewFromString(balance.BalanceStr)
	if err != nil {
		return false, err
	}

	if bal.Cmp(limit.Mul(decimal.NewFromInt(2))) < 0 {
		return false, nil
	}

	h.amount = bal.Sub(limit)

	return true, nil
}

func (h *coinHandler) checkTransferring(ctx context.Context) (bool, error) {
	exist, err := txmwcli.ExistTxConds(ctx, &txmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		AccountID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.userBenefitColdAccount.AccountID},
		States: &basetypes.Uint32SliceVal{Op: cruder.IN, Value: []uint32{
			uint32(basetypes.TxState_TxStateCreated),
			uint32(basetypes.TxState_TxStateWait),
			uint32(basetypes.TxState_TxStateTransferring),
		}},
		Type: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxType_TxLimitation)},
	})
	if err != nil {
		return false, err
	}
	if exist {
		return true, nil
	}

	txs, _, err := txmwcli.GetTxs(ctx, &txmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		AccountID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.userBenefitColdAccount.AccountID},
		State:      &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxState_TxStateSuccessful)},
		Type:       &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxType_TxLimitation)},
	}, int32(0), int32(1))
	if err != nil {
		return false, err
	}
	if len(txs) == 0 {
		return false, nil
	}
	const coolDown = timedef.SecondsPerHour
	if txs[0].CreatedAt+coolDown > uint32(time.Now().Unix()) {
		return true, nil
	}
	return false, nil
}

func (h *coinHandler) final(ctx context.Context, err *error) {
	persistentCoin := &types.PersistentCoin{
		Coin:          h.Coin,
		FromAccountID: h.userBenefitHotAccount.AccountID,
		FromAddress:   h.userBenefitHotAccount.Address,
		ToAccountID:   h.userBenefitColdAccount.AccountID,
		ToAddress:     h.userBenefitColdAccount.Address,
		Amount:        h.amount.String(),
		FeeAmount:     decimal.NewFromInt(0).String(),
		Error:         *err,
	}

	if *err == nil {
		h.persistent <- persistentCoin
	} else {
		h.notif <- persistentCoin
	}
}

func (h *coinHandler) exec(ctx context.Context) error {
	var err error
	var yes bool
	defer h.final(ctx, &err)

	h.userBenefitHotAccount, err = h.getPlatformAccount(ctx, basetypes.AccountUsedFor_UserBenefitHot)
	if err != nil {
		return err
	}
	h.userBenefitColdAccount, err = h.getPlatformAccount(ctx, basetypes.AccountUsedFor_UserBenefitCold)
	if err != nil {
		return err
	}
	if yes, err = h.checkBalanceLimitation(ctx); err != nil || !yes {
		return err
	}
	if yes, err = h.checkTransferring(ctx); err != nil || yes {
		return err
	}

	return nil
}
