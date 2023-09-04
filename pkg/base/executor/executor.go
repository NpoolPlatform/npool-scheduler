package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

type Executor interface {
	Feed(interface{})
	Finalize(ctx context.Context)
	Notif() chan interface{}
	Persistent() chan interface{}
	Feeder() chan interface{}
}

type Exec interface {
	Exec(context.Context, interface{}, chan interface{}, chan interface{}, chan interface{}) error
}

type handler struct {
	persistent chan interface{}
	notif      chan interface{}
	feeder     chan interface{}
	exec       Exec
	w          *watcher.Watcher
	subsystem  string
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent, notif chan interface{}, exec Exec, subsystem string) Executor {
	e := &handler{
		feeder:     make(chan interface{}),
		persistent: persistent,
		notif:      notif,
		w:          watcher.NewWatcher(),
		exec:       exec,
		subsystem:  subsystem,
	}

	go action.Watch(ctx, cancel, e.run, e.paniced)
	return e
}

func (e *handler) handler(ctx context.Context) bool {
	select {
	case ent := <-e.feeder:
		if err := e.exec.Exec(ctx, ent, e.feeder, e.persistent, e.notif); err != nil {
			logger.Sugar().Errorw(
				"handler",
				"State", "Exec",
				"Subsystem", e.subsystem,
				"Error", err,
			)
		}
		return false
	case <-e.w.CloseChan():
		close(e.w.ClosedChan())
		return true
	}
}

func (e *handler) run(ctx context.Context) {
	for {
		if b := e.handler(ctx); b {
			break
		}
	}
}

func (e *handler) paniced(ctx context.Context) {
	close(e.w.CloseChan())
}

func (e *handler) Finalize(ctx context.Context) {
	if e != nil && e.w != nil {
		e.w.Shutdown(ctx)
	}
}

func (e *handler) Feed(ent interface{}) {
	e.feeder <- ent
}

func (e *handler) Persistent() chan interface{} {
	return e.persistent
}

func (e *handler) Notif() chan interface{} {
	return e.notif
}

func (e *handler) Feeder() chan interface{} {
	return e.feeder
}
