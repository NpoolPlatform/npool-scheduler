package transferring

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/transferring/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/transferring/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/transferring/sentinel"
)

type handler struct {
	persistent   chan interface{}
	notif        chan interface{}
	executor     baseexecutor.Executor
	persistenter basepersistent.Persistent
	w            *watcher.Watcher
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	h = &handler{
		persistent: make(chan interface{}),
		notif:      make(chan interface{}),
		w:          watcher.NewWatcher(),
	}

	sentinel.Initialize(ctx, cancel)
	h.executor = executor.NewExecutor(ctx, cancel, h.persistent, h.notif)
	h.persistenter = persistent.NewPersistent(ctx, cancel)

	go action.Watch(ctx, cancel, h.run)
}

func (h *handler) execTx(ctx context.Context, tx interface{}) error {
	h.executor.Feed(tx)
	return nil
}

func (h *handler) persistentTx(ctx context.Context, tx interface{}) error {
	h.persistenter.Feed(tx)
	return nil
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case tx := <-sentinel.Exec():
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
		close(h.persistent)
		h.executor.Finalize()
		h.persistenter.Finalize()
	}
}
