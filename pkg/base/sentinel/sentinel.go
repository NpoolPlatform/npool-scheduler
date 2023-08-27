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
	InitScan(context.Context, chan interface{}) error
	Scan(context.Context, chan interface{}) error
}

type handler struct {
	w            *watcher.Watcher
	exec         chan interface{}
	scanner      Scanner
	scanInterval time.Duration
	subsystem    string
}

func NewSentinel(ctx context.Context, cancel context.CancelFunc, scanner Scanner, scanInterval time.Duration, subsystem string) Sentinel {
	h := &handler{
		w:            watcher.NewWatcher(),
		exec:         make(chan interface{}),
		scanner:      scanner,
		scanInterval: scanInterval,
		subsystem:    subsystem,
	}
	go action.Watch(ctx, cancel, h.run)
	scanner.InitScan(ctx, h.exec)
	return h
}
func (h *handler) Exec() chan interface{} {
	return h.exec
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case <-time.After(h.scanInterval):
		if err := h.scanner.Scan(ctx, h.exec); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Scan",
				"Subsystem", h.subsystem,
				"Error", err,
			)
		}
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"handler",
			"State", "Done",
			"Subsystem", h.subsystem,
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