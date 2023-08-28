package executor

import (
	"context"
	"fmt"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/finish/types"

	"github.com/shopspring/decimal"
)

type accountHandler struct {
	*depositaccmwpb.Account
	persistent chan interface{}
	notif      chan interface{}
	coin       *coinmwpb.Coin
	outcoming  decimal.Decimal
	txFinished bool
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

func (h *accountHandler) checkTransfer(ctx context.Context) error {
	tx, err := txmwcli.GetTx(ctx, h.CollectingTID)
	if err != nil {
		return err
	}
	if tx == nil {
		h.txFinished = true
		return nil
	}

	switch tx.State {
	case basetypes.TxState_TxStateSuccessful:
		h.outcoming = decimal.RequireFromString(tx.Amount)
		fallthrough //nolint
	case basetypes.TxState_TxStateFail:
		h.txFinished = true
	default:
		return nil
	}

	return nil
}

func (h *accountHandler) final(ctx context.Context, err *error) {
	if !h.txFinished && *err == nil {
		return
	}

	persistentAccount := &types.PersistentAccount{
		Account: h.Account,
		Error:   *err,
	}
	if h.outcoming.Cmp(decimal.NewFromInt(0)) > 0 {
		outcoming := h.outcoming.String()
		persistentAccount.CollectOutcoming = &outcoming
	}

	if *err == nil {
		h.persistent <- persistentAccount
	} else {
		h.notif <- persistentAccount
	}
}

func (h *accountHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if err = h.getCoin(ctx); err != nil {
		return err
	}
	if err := h.checkTransfer(ctx); err != nil {
		return err
	}
	return nil
}
