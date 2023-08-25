package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

type Sentinel interface {
	Exec() chan interface{}
	Finalize()
}

type Scanner interface {
	Scan(context.Context) error
}

type handler struct {
	w            *watcher.Watcher
	exec         chan interface{}
	scanner      Scanner
	scanInterval time.Duration
}

func NewSentinel(ctx context.Context, cancel context.CancelFunc, scanner Scanner, scanInterval time.Duration) Sentinel {
	h := &handler{
		w:            watcher.NewWatcher(),
		exec:         make(chan interface{}),
		scanner:      scanner,
		scanInterval: scanInterval,
	}
	go action.Watch(ctx, cancel, h.run)
	return h
}
func (h *handler) Exec() chan interface{} {
	return h.exec
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case <-time.After(h.scanInterval):
		if err := h.scanner.Scan(ctx); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Scan",
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

func (h *handler) Finalize() {
	if h.w != nil {
		h.w.Shutdown()
	}
	close(h.exec)
}
