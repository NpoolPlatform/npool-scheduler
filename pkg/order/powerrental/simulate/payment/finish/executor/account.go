package executor

import (
	"context"
	"fmt"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/finish/types"

	"github.com/google/uuid"
)

type accountHandler struct {
	*payaccmwpb.Account
	persistent chan interface{}
	done       chan interface{}
	coin       *coinmwpb.Coin
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
	if _, err := uuid.Parse(h.CollectingTID); err != nil {
		return err
	}
	if h.CollectingTID == uuid.Nil.String() {
		return nil
	}

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
		fallthrough //nolint
	case basetypes.TxState_TxStateFail:
		h.txFinished = true
	default:
		return nil
	}

	return nil
}

//nolint:gocritic
func (h *accountHandler) final(ctx context.Context, err *error) {
	persistentAccount := &types.PersistentAccount{
		Account: h.Account,
		Error:   *err,
	}

	if !h.txFinished && *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentAccount, h.done)
		return
	}
	if h.txFinished {
		asyncfeed.AsyncFeed(ctx, persistentAccount, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentAccount, h.done)
}

//nolint:gocritic
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
