package executor

import (
	"context"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/types"
)

type txHandler struct {
	*txmwpb.Tx
	persistent chan interface{}
	newState   basetypes.TxState
}

func (h *txHandler) checkWait(ctx context.Context) error {
	exist, err := txmwcli.ExistTxConds(ctx, &txmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		AccountID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.FromAccountID},
		States: &basetypes.Uint32SliceVal{Op: cruder.IN, Value: []uint32{
			uint32(basetypes.TxState_TxStateWait),
			uint32(basetypes.TxState_TxStateTransferring),
		}},
	})
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	h.newState = basetypes.TxState_TxStateWait
	return nil
}

func (h *txHandler) final() {
	if h.newState == h.State {
		return
	}

	persistentTx := &types.PersistentTx{
		Tx: h.Tx,
	}
	asyncfeed.AsyncFeed(persistentTx, h.persistent)
}

func (h *txHandler) exec(ctx context.Context) error {
	defer h.final()

	if err := h.checkWait(ctx); err != nil {
		return err
	}
	return nil
}
