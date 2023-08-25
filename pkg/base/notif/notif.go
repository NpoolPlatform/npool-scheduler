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
	feeder chan interface{}
	w      *watcher.Watcher
	notify Notify
}

func NewNotif(ctx context.Context, cancel context.CancelFunc, notify Notify) Notif {
	p := &handler{
		feeder: make(chan interface{}),
		w:      watcher.NewWatcher(),
		notify: notify,
	}

	go action.Watch(ctx, cancel, p.run)
	return p
}

func (p *handler) handler(ctx context.Context) bool {
	select {
	case ent := <-p.feeder:
		if err := p.notify.Notify(ctx, ent); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Notify",
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
