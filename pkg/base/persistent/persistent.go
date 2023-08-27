package persistent

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

type Persistent interface {
	Feed(interface{})
	Finalize()
}

type Persistenter interface {
	Update(context.Context, interface{}, chan interface{}, chan interface{}) error
}

type handler struct {
	feeder       chan interface{}
	notif        chan interface{}
	w            *watcher.Watcher
	persistenter Persistenter
	subsystem    string
}

func NewPersistent(ctx context.Context, cancel context.CancelFunc, notif chan interface{}, persistenter Persistenter, subsystem string) Persistent {
	p := &handler{
		feeder:       make(chan interface{}),
		notif:        notif,
		w:            watcher.NewWatcher(),
		persistenter: persistenter,
		subsystem:    subsystem,
	}

	go action.Watch(ctx, cancel, p.run)
	return p
}

func (p *handler) handler(ctx context.Context) bool {
	select {
	case ent := <-p.feeder:
		if err := p.persistenter.Update(ctx, ent, p.feeder, p.notif); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Update",
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
