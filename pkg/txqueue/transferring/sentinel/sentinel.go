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
}

func (h *handler) Scan(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		txs, _, err := txmwcli.GetTxs(ctx, &txmwpb.Conds{
			State: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxState_TxStateTransferring)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			return nil
		}

		for _, tx := range txs {
			h.Exec() <- tx
		}

		offset += limit
	}
}

func Exec() chan interface{} {
	return h.Exec()
}

func Finalize() {
	if h != nil {
		h.Finalize()
	}
}
