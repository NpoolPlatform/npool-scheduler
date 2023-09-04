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
	Trigger(interface{})
	Finalize(ctx context.Context)
}

type Scanner interface {
	InitScan(context.Context, chan interface{}) error
	Scan(context.Context, chan interface{}) error
	TriggerScan(context.Context, interface{}, chan interface{}) error
	ObjectID(interface{}) string
}

type handler struct {
	w            *watcher.Watcher
	exec         chan interface{}
	trigger      chan interface{}
	scanner      Scanner
	scanInterval time.Duration
	subsystem    string
}

func NewSentinel(ctx context.Context, cancel context.CancelFunc, scanner Scanner, scanInterval time.Duration, subsystem string) Sentinel {
	h := &handler{
		w:            watcher.NewWatcher(),
		exec:         make(chan interface{}),
		trigger:      make(chan interface{}),
		scanner:      scanner,
		scanInterval: scanInterval,
		subsystem:    subsystem,
	}
	go action.Watch(ctx, cancel, h.run, h.paniced)
	go scanner.InitScan(ctx, h.exec) //nolint
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
	case cond := <-h.trigger:
		if err := h.scanner.TriggerScan(ctx, cond, h.exec); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Scan",
				"Subsystem", h.subsystem,
				"Error", err,
			)
		}
		return false
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

func (h *handler) paniced(ctx context.Context) {
	close(h.w.ClosedChan())
}

func (h *handler) Trigger(cond interface{}) {
	h.trigger <- cond
}

func (h *handler) Finalize(ctx context.Context) {
	if h.w != nil {
		h.w.Shutdown(ctx)
	}
}
