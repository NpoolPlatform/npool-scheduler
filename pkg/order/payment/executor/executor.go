package executor

import (
	"context"
	"fmt"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct {
	baseexecutor.Executor
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent, notif chan interface{}) baseexecutor.Executor {
	h := &handler{}
	return baseexecutor.NewExecutor(ctx, cancel, persistent, notif, h)
}

func (e *handler) Exec(ctx context.Context, order interface{}) error {
	_order, ok := order.(*ordermwpb.Order)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	h := &orderHandler{
		Order:           _order,
		retryOrder:      e.Feeder(),
		persistentOrder: e.Persistent(),
		notifOrder:      e.Notif(),
	}
	return h.exec(ctx)
}
