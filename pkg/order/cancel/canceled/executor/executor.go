package executor

import (
	"context"
	"fmt"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, order interface{}, retry, persistent, notif chan interface{}) error {
	_order, ok := order.(*ordermwpb.Order)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	h := &orderHandler{
		Order:      _order,
		persistent: persistent,
		notif:      notif,
	}
	return h.exec(ctx)
}