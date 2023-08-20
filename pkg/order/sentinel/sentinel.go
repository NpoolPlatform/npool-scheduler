package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

var w *watcher.Watcher

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	go action.Watch(ctx, cancel, run)
}

func handler(ctx context.Context, w *watcher.Watcher) bool {
	const scanInterval = 30 * time.Second
	ticker := time.NewTicker(scanInterval)

	select {
	case <-ticker.C:
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"run",
			"State", "Done",
			"Error", ctx.Err(),
		)
		close(w.ClosedChan())
		return true
	case <-w.CloseChan():
		close(w.ClosedChan())
		return true
	}
}

func run(ctx context.Context) {
	w = watcher.NewWatcher()

	for {
		if b := handler(ctx, w); b {
			break
		}
	}
}

func Finalize() {
	if w != nil {
		w.Shutdown()
	}
}
