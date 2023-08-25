package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

type Scanner interface {
	Scan(context.Context) error
}

type Sentinel struct {
	w            *watcher.Watcher
	exec         chan interface{}
	scanner      Scanner
	scanInterval time.Duration
}

func NewSentinel(ctx context.Context, cancel context.CancelFunc, scanner Scanner, scanInterval time.Duration) *Sentinel {
	h := &Sentinel{
		w:            watcher.NewWatcher(),
		exec:         make(chan interface{}),
		scanner:      scanner,
		scanInterval: scanInterval,
	}
	go action.Watch(ctx, cancel, h.run)
	return h
}
func (h *Sentinel) Exec() chan interface{} {
	return h.exec
}

func (h *Sentinel) handler(ctx context.Context) bool {
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

func (h *Sentinel) run(ctx context.Context) {
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func (h *Sentinel) Finalize() {
	if h.w != nil {
		h.w.Shutdown()
	}
	close(h.exec)
}
