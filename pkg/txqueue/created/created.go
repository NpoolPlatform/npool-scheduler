package created

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/sentinel"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/types"
)

type handler struct {
	exec         chan *txmwpb.Tx
	persistent   chan *types.PersistentTx
	executor     executor.Executor
	persistenter persistent.Persistent
	w            *watcher.Watcher
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	h = &handler{
		exec:       make(chan *txmwpb.Tx),
		persistent: make(chan *types.PersistentTx),
		w:          watcher.NewWatcher(),
	}

	sentinel.Initialize(ctx, cancel, h.exec)
	h.executor = executor.NewExecutor(ctx, cancel, h.persistent)
	h.persistenter = persistent.NewPersistent(ctx, cancel)

	go action.Watch(ctx, cancel, h.run)
}

func (h *handler) execTx(ctx context.Context, tx *txmwpb.Tx) error {
	h.executor.Feed(tx)
	return nil
}

func (h *handler) persistentTx(ctx context.Context, tx *types.PersistentTx) error {
	h.persistenter.Feed(tx)
	return nil
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case tx := <-h.exec:
		if err := h.execTx(ctx, tx); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "execTx",
				"Error", err,
			)
		}
		return false
	case tx := <-h.persistent:
		if err := h.persistentTx(ctx, tx); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "persistentTx",
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
		close(h.w.ClosedChan())
		return true
	case <-h.w.CloseChan():
		close(h.w.ClosedChan())
		return true
	}
}

func (h *handler) run(ctx context.Context) {
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func Finalize() {
	if h != nil && h.w != nil {
		h.w.Shutdown()
		close(h.exec)
		close(h.persistent)
		h.executor.Finalize()
		h.persistenter.Finalize()
	}
}
