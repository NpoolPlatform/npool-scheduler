package executor

import (
	"context"
	"fmt"

	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, tx interface{}, persistent, notif, done chan interface{}) error {
	_tx, ok := tx.(*txmwpb.Tx)
	if !ok {
		return fmt.Errorf("invalid tx")
	}
	h := &txHandler{
		Tx:         _tx,
		persistent: persistent,
		notif:      notif,
		done:       done,
	}
	return h.exec(ctx)
}
