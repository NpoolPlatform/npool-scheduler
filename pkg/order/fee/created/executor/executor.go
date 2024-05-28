package executor

import (
	"context"
	"fmt"

	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, order interface{}, persistent, notif, done chan interface{}) error {
	_order, ok := order.(*feeordermwpb.FeeOrder)
	if !ok {
		return fmt.Errorf("invalid feeorder")
	}

	h := &orderHandler{
		FeeOrder:   _order,
		persistent: persistent,
	}
	return h.exec(ctx)
}
