package executor

import (
	"context"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type Executor interface {
	Feed(*ordermwpb.Order)
}

type exec struct {
	persistent chan *ordermwpb.Order
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent chan *ordermwpb.Order) Executor {
	return &exec{
		persistent: persistent,
	}
}

func (e *exec) Feed(order *ordermwpb.Order) {
}
