package persistent

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
)

type Persistenter interface {
	Persistent(context.Context, interface{}) error
}

type Persistent struct {
	feeder       chan interface{}
	w            *watcher.Watcher
	persistenter Persistenter
}

func NewPersistent(ctx context.Context, cancel context.CancelFunc, persistenter Persistenter) *Persistent {
	p := &Persistent{
		feeder:       make(chan interface{}),
		w:            watcher.NewWatcher(),
		persistenter: persistenter,
	}

	go action.Watch(ctx, cancel, p.run)
	return p
}

func (p *Persistent) handler(ctx context.Context) bool {
	select {
	case ent := <-p.feeder:
		if err := p.persistenter.Persistent(ctx, ent); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "Persistent",
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

func (p *Persistent) run(ctx context.Context) {
	for {
		if b := p.handler(ctx); b {
			break
		}
	}
}

func (p *Persistent) Finalize() {
	if p != nil && p.w != nil {
		p.w.Shutdown()
		close(p.feeder)
	}
}

func (p *Persistent) Feed(ent interface{}) {
	p.feeder <- ent
}
