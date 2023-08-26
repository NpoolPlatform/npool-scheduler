package executor

import (
	"context"
	"fmt"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/transferring/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type txHandler struct {
	*txmwpb.Tx
	persistent chan interface{}
	newState   basetypes.TxState
	txExtra    string
	txCID      string
}

func (h *txHandler) checkTransfer(ctx context.Context) error {
	tx, err := sphinxproxycli.GetTransaction(ctx, h.ID)
	if err != nil {
		switch status.Code(err) {
		case codes.InvalidArgument:
			fallthrough //nolint
		case codes.NotFound:
			fallthrough //nolint
		case codes.Aborted:
			h.newState = basetypes.TxState_TxStateFail
			return nil
		default:
			return err
		}
	} else if tx == nil {
		return fmt.Errorf("invalid transactionid")
	}

	switch tx.TransactionState {
	case sphinxproxypb.TransactionState_TransactionStateFail:
		h.newState = basetypes.TxState_TxStateFail
	case sphinxproxypb.TransactionState_TransactionStateDone:
		h.newState = basetypes.TxState_TxStateSuccessful
		h.txCID = tx.CID
		if tx.CID == "" {
			h.txCID = "(successful without CID)"
			h.newState = basetypes.TxState_TxStateFail
		}
	}
	return nil
}

func (h *txHandler) final(ctx context.Context) {
	if h.newState == h.State {
		return
	}

	persistentTx := &types.PersistentTx{
		Tx:         h.Tx,
		NewTxState: h.newState,
		TxExtra:    h.txExtra,
		TxCID:      h.txCID,
	}
	h.persistent <- persistentTx
}

func (h *txHandler) exec(ctx context.Context) error {
	h.newState = h.State

	defer h.final(ctx)

	if err := h.checkTransfer(ctx); err != nil {
		return err
	}

	return nil
}
