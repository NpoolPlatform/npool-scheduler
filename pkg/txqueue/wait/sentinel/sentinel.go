package sentinel

import (
	"context"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) feedTx(ctx context.Context, tx *txmwpb.Tx, exec chan interface{}) error {
	state := basetypes.TxState_TxStateWaitCheck
	if _, err := txmwcli.UpdateTx(ctx, &txmwpb.TxReq{
		ID:    &tx.ID,
		State: &state,
	}); err != nil {
		return err
	}
	exec <- tx
	return nil
}

func (h *handler) scanTxs(ctx context.Context, state basetypes.TxState, exec chan interface{}) error {
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
			if err := h.feedTx(ctx, tx, exec); err != nil {
				return err
			}
			ignores[tx.FromAccountID] = struct{}{}
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	return h.scanTxs(ctx, basetypes.TxState_TxStateWait, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanTxs(ctx, basetypes.TxState_TxStateWaitCheck, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}
