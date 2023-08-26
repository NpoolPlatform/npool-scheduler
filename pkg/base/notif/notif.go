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
	Notify(context.Context, interface{}) error
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
	select {
	case ent := <-p.feeder:
		if p.notify == nil {
			return false
		}
		if err := p.notify.Notify(ctx, ent); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Notify",
				"Subsystem", p.subsystem,
				"Error", err,
			)
		}
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"handler",
			"State", "Done",
			"Subsystem", p.subsystem,
			"Error", ctx.Err(),
		)
		close(p.w.ClosedChan())
		return true
	case <-p.w.CloseChan():
		close(p.w.ClosedChan())
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
		close(p.feeder)
	}
}

func (p *handler) Feed(ent interface{}) {
	p.feeder <- ent
}
