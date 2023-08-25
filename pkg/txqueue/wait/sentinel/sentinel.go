package sentinel

import (
	"context"
	"time"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct {
	basesentinel.Sentinel
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	h = &handler{
		Sentinel: basesentinel.NewSentinel(ctx, cancel, h, time.Minute),
	}
	h.scanTxs(ctx, basetypes.TxState_TxStateWaitCheck)
}

func (h *handler) feedTx(ctx context.Context, tx *txmwpb.Tx) error {
	state := basetypes.TxState_TxStateWaitCheck
	if _, err := txmwcli.UpdateTx(ctx, &txmwpb.TxReq{
		ID:    &tx.ID,
		State: &state,
	}); err != nil {
		return err
	}
	h.Exec() <- tx
	return nil
}

func (h *handler) scanTxs(ctx context.Context, state basetypes.TxState) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		txs, _, err := txmwcli.GetTxs(ctx, &txmwpb.Conds{
			State: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			return nil
		}

		ignores := map[string]struct{}{}
		for _, tx := range txs {
			if _, ok := ignores[tx.FromAccountID]; ok {
				continue
			}
			if err := h.feedTx(ctx, tx); err != nil {
				return err
			}
			ignores[tx.FromAccountID] = struct{}{}
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context) error {
	return h.scanTxs(ctx, basetypes.TxState_TxStateWait)
}

func Exec() chan interface{} {
	return h.Exec()
}

func Finalize() {
	if h != nil {
		h.Finalize()
	}
}
