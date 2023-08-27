package executor

import (
	"context"
	"fmt"

	depositaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/transfer/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type accountHandler struct {
	*depositaccmwpb.Account
	persistent     chan interface{}
	notif          chan interface{}
	incoming       decimal.Decimal
	outcoming      decimal.Decimal
	amount         decimal.Decimal
	coin           *coinmwpb.Coin
	collectAccount *pltfaccmwpb.Account
}

func (h *accountHandler) getCoin(ctx context.Context) error {
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

func (h *accountHandler) recheckAccountLock(ctx context.Context) (bool, error) {
	account, err := depositaccmwcli.GetAccount(ctx, h.ID)
	if err != nil {
		return false, err
	}
	if account == nil {
		return false, fmt.Errorf("invalid account")
	}
	return account.Locked, nil
}

func (h *accountHandler) checkBalance(ctx context.Context) error {
	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.coin.Name,
		Address: h.Address,
	})
	if err != nil {
		return err
	}
	if bal == nil {
		return fmt.Errorf("invalid balance")
	}

	balance, err := decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}
	if balance.Cmp(h.incoming.Sub(h.outcoming)) <= 0 {
		return nil
	}
	h.amount = balance.Sub(h.incoming.Sub(h.outcoming))
	return nil
}

func (h *accountHandler) getCollectAccount(ctx context.Context) error {
	account, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.coin.ID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountUsedFor_PaymentCollector)},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return nil
	}
	if account == nil {
		return fmt.Errorf("invalid collect account")
	}
	h.collectAccount = account
	return nil
}

func (h *accountHandler) final(ctx context.Context, err *error) {
	if h.amount.Cmp(decimal.NewFromInt(0)) <= 0 && *err == nil {
		return
	}

	persistentAccount := &types.PersistentAccount{
		Account:          h.Account,
		CollectAmount:    h.amount.String(),
		FeeAmount:        decimal.NewFromInt(0).String(),
		DepositAccountID: h.AccountID,
		DepositAddress:   h.Address,
		CollectAccountID: h.collectAccount.AccountID,
		CollectAddress:   h.collectAccount.Address,
		Error:            *err,
	}

	if *err == nil {
		h.persistent <- persistentAccount
	} else {
		h.notif <- persistentAccount
	}
}

func (h *accountHandler) exec(ctx context.Context) error {
	if h.Locked {
		return nil
	}

	var err error
	var locked bool

	h.incoming, err = decimal.NewFromString(h.Incoming)
	if err != nil {
		return err
	}
	h.outcoming, err = decimal.NewFromString(h.Outcoming)
	if err != nil {
		return err
	}

	defer h.final(ctx, &err)

	if err = h.getCoin(ctx); err != nil {
		return err
	}
	if err := h.getCollectAccount(ctx); err != nil {
		return err
	}
	if err = accountlock.Lock(h.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(h.AccountID)
	}()
	if locked, err = h.recheckAccountLock(ctx); err != nil || locked {
		return err
	}
	if err = h.checkBalance(ctx); err != nil {
		return err
	}
	return nil
}