package executor

import (
	"context"

	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	// types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/types"
)

type txHandler struct {
	*txmwpb.Tx
	persistent chan interface{}
}

func (h *txHandler) exec(ctx context.Context) error {
	return nil
}
