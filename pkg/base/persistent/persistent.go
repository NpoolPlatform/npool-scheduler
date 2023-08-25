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
	Update(context.Context, interface{}) error
}

type handler struct {
	feeder       chan interface{}
	w            *watcher.Watcher
	persistenter Persistenter
}

func NewPersistent(ctx context.Context, cancel context.CancelFunc, persistenter Persistenter) Persistent {
	p := &handler{
		feeder:       make(chan interface{}),
		w:            watcher.NewWatcher(),
		persistenter: persistenter,
	}

	go action.Watch(ctx, cancel, p.run)
	return p
}

func (p *handler) handler(ctx context.Context) bool {
	select {
	case ent := <-p.feeder:
		if err := p.persistenter.Update(ctx, ent); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Update",
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
