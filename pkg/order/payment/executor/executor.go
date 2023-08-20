package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type Executor interface {
	Feed(*ordermwpb.Order)
	Finalize()
}

type exec struct {
	persistent chan *ordermwpb.Order
	newOrder   chan *ordermwpb.Order
	w          *watcher.Watcher
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent chan *ordermwpb.Order) Executor {
	e := &exec{
		persistent: persistent,
		newOrder:   make(chan *ordermwpb.Order),
		w:          watcher.NewWatcher(),
	}

	go action.Watch(ctx, cancel, e.run)
	return e
}

func (e *exec) execOrder(ctx context.Context, order *ordermwpb.Order) error {
	return nil
}

func (e *exec) handler(ctx context.Context) bool {
	select {
	case order := <-e.newOrder:
		if err := e.execOrder(ctx, order); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "execOrder",
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

func (e *exec) run(ctx context.Context) {
	for {
		if b := e.handler(ctx); b {
			break
		}
	}
}

func (e *exec) Finalize() {
	if e != nil && e.w != nil {
		e.w.Shutdown()
		close(e.newOrder)
	}
}

func (e *exec) Feed(order *ordermwpb.Order) {
	e.newOrder <- order
}
