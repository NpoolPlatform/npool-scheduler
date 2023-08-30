package executor

import (
	"context"
	"fmt"
	"sync"

	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct {
	mutex *sync.Mutex
}

func NewExecutor(mutex *sync.Mutex) baseexecutor.Exec {
	return &handler{
		mutex: mutex,
	}
}

func (e *handler) Exec(ctx context.Context, tx interface{}, retry, persistent, notif chan interface{}) error {
	_tx, ok := tx.(*txmwpb.Tx)
	if !ok {
		return fmt.Errorf("invalid tx")
	}
	h := &txHandler{
		Tx:         _tx,
		persistent: persistent,
		retry:      retry,
		mutex:      e.mutex,
	}
	return h.exec(ctx)
}
