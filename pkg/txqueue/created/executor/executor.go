package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/types"
)

type Executor interface {
	Feed(*txmwpb.Tx)
	Finalize()
}

type exec struct {
	persistent chan *types.PersistentTx
	feeder     chan *txmwpb.Tx
	w          *watcher.Watcher
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent chan *types.PersistentTx) Executor {
	e := &exec{
		feeder:     make(chan *txmwpb.Tx),
		persistent: persistent,
		w:          watcher.NewWatcher(),
	}

	go action.Watch(ctx, cancel, e.run)
	return e
}

func (e *exec) execTx(ctx context.Context, tx *txmwpb.Tx) error {
	h := &txHandler{
		Tx:         tx,
		persistent: e.persistent,
	}
	return h.exec(ctx)
}

func (e *exec) handler(ctx context.Context) bool {
	select {
	case tx := <-e.feeder:
		if err := e.execTx(ctx, tx); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "execTx",
				"Error", err,
			)
		}
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"handler",
			"State", "Done",
			"Error", ctx.Err(),
		)
		close(e.w.ClosedChan())
		return true
	case <-e.w.CloseChan():
		close(e.w.ClosedChan())
		return true
	}
}

func (e *exec) run(ctx context.Context) {
	for {
		if b := e.handler(ctx); b {
			break
		}
	}
}

func (e *exec) Finalize() {
	if e != nil && e.w != nil {
		e.w.Shutdown()
		close(e.feeder)
	}
}

func (e *exec) Feed(tx *txmwpb.Tx) {
	e.feeder <- tx
}
