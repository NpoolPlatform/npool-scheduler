package executor

import (
	"context"
	"fmt"

	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct {
	baseexecutor.Executor
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent, notif chan interface{}) baseexecutor.Executor {
	h := &handler{}
	return baseexecutor.NewExecutor(ctx, cancel, persistent, notif, h)
}

func (e *handler) Exec(ctx context.Context, tx interface{}) error {
	_tx, ok := tx.(*txmwpb.Tx)
	if !ok {
		return fmt.Errorf("invalid tx")
	}
	h := &txHandler{
		Tx:         _tx,
		persistent: e.Persistent(),
	}
	return h.exec(ctx)
}
