package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/types"
)

type Executor interface {
	Feed(*ordermwpb.Order)
	Finalize()
}

type exec struct {
	persistent chan *types.PersistentOrder
	notif      chan *types.PersistentOrder
	newOrder   chan *ordermwpb.Order
	w          *watcher.Watcher
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent, notif chan *types.PersistentOrder) Executor {
	e := &exec{
		newOrder:   make(chan *ordermwpb.Order),
		persistent: persistent,
		notif:      notif,
		w:          watcher.NewWatcher(),
	}

	go action.Watch(ctx, cancel, e.run)
	return e
}

func (e *exec) execOrder(ctx context.Context, order *ordermwpb.Order) error {
	h := &orderHandler{
		Order:           order,
		retryOrder:      e.newOrder,
		persistentOrder: e.persistent,
		notifOrder:      e.notif,
	}
	return h.exec(ctx)
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
