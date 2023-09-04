package notif

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

type Notif interface {
	Feed(interface{})
	Finalize()
}

type Notify interface {
	Notify(context.Context, interface{}, chan interface{}) error
}

type handler struct {
	feeder    chan interface{}
	w         *watcher.Watcher
	notify    Notify
	subsystem string
}

func NewNotif(ctx context.Context, cancel context.CancelFunc, notify Notify, subsystem string) Notif {
	p := &handler{
		feeder:    make(chan interface{}),
		w:         watcher.NewWatcher(),
		notify:    notify,
		subsystem: subsystem,
	}

	go action.Watch(ctx, cancel, p.run)
	return p
}

func (p *handler) handler(ctx context.Context) bool {
	closed := false
	defer func() {
		if err := recover(); err != nil {
			if !closed {
				close(p.w.ClosedChan())
			}
		}
	}()
	select {
	case ent := <-p.feeder:
		if p.notify == nil {
			return false
		}
		if err := p.notify.Notify(ctx, ent, p.feeder); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Notify",
				"Subsystem", p.subsystem,
				"Error", err,
			)
		}
		return false
	case <-p.w.CloseChan():
		close(p.w.ClosedChan())
		closed = true
		return true
	}
}

func (p *handler) run(ctx context.Context) {
	for {
		if b := p.handler(ctx); b {
			break
		}
	}
}

func (p *handler) Finalize() {
	if p != nil && p.w != nil {
		p.w.Shutdown()
	}
}

func (p *handler) Feed(ent interface{}) {
	p.feeder <- ent
}
