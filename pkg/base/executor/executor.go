package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

type Executor interface {
	Feed(interface{})
	Finalize()
	Notif() chan interface{}
	Persistent() chan interface{}
	Feeder() chan interface{}
}

type Exec interface {
	Exec(context.Context, interface{}) error
}

type handler struct {
	persistent chan interface{}
	notif      chan interface{}
	feeder     chan interface{}
	exec       Exec
	w          *watcher.Watcher
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent, notif chan interface{}, exec Exec) Executor {
	e := &handler{
		feeder:     make(chan interface{}),
		persistent: persistent,
		notif:      notif,
		w:          watcher.NewWatcher(),
		exec:       exec,
	}

	go action.Watch(ctx, cancel, e.run)
	return e
}

func (e *handler) handler(ctx context.Context) bool {
	select {
	case ent := <-e.feeder:
		if err := e.exec.Exec(ctx, ent); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Exec",
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

func (e *handler) run(ctx context.Context) {
	for {
		if b := e.handler(ctx); b {
			break
		}
	}
}

func (e *handler) Finalize() {
	if e != nil && e.w != nil {
		e.w.Shutdown()
		close(e.feeder)
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
