package payment

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/sentinel"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/types"
)

type handler struct {
	exec       chan *ordermwpb.Order
	persistent chan *types.PersistentOrder
	notif      chan *types.PersistentOrder
	execIndex  int
	executors  []executor.Executor
	w          *watcher.Watcher
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	h = &handler{
		exec:       make(chan *ordermwpb.Order),
		persistent: make(chan *types.PersistentOrder),
		notif:      make(chan *types.PersistentOrder),
		w:          watcher.NewWatcher(),
	}

	sentinel.Initialize(ctx, cancel, h.exec)

	const executors = 4
	for i := 0; i < executors; i++ {
		pe := executor.NewExecutor(ctx, cancel, h.persistent, h.notif)
		h.executors = append(h.executors, pe)
	}

	persistent.Initialize(ctx, cancel)

	go action.Watch(ctx, cancel, h.run)
}

func (h *handler) execOrder(ctx context.Context, order *ordermwpb.Order) error {
	h.executors[h.execIndex].Feed(order)
	h.execIndex += 1
	h.execIndex = h.execIndex % len(h.executors)
	return nil
}

func (h *handler) persistentOrder(ctx context.Context, order *types.PersistentOrder) error {
	logger.Sugar().Infow(
		"persistentOrder",
		"OrderID", order.ID,
		"Error", order.Error,
	)
	return nil
}

func (h *handler) notifOrder(ctx context.Context, order *types.PersistentOrder) error {
	logger.Sugar().Infow(
		"notifOrder",
		"OrderID", order.ID,
		"Error", order.Error,
	)
	return nil
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case order := <-h.exec:
		if err := h.execOrder(ctx, order); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "execOrder",
				"Error", err,
			)
		}
		return false
	case order := <-h.persistent:
		if err := h.persistentOrder(ctx, order); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "persistentOrder",
				"Error", err,
			)
		}
		return false
	case order := <-h.notif:
		if err := h.notifOrder(ctx, order); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "persistentOrder",
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
		close(h.w.ClosedChan())
		return true
	case <-h.w.CloseChan():
		close(h.w.ClosedChan())
		return true
	}
}

func (h *handler) run(ctx context.Context) {
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func Finalize() {
	if h != nil && h.w != nil {
		h.w.Shutdown()
		close(h.exec)
		close(h.persistent)
	}
}
