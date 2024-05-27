package executor

import (
	"context"
	"fmt"

	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, powerRentalOrder interface{}, persistent, notif, done chan interface{}) error {
	_powerRentalOrder, ok := powerRentalOrder.(*powerrentalordermwpb.PowerRentalOrder)
	if !ok {
		return fmt.Errorf("invalid powerrentalorder")
	}

	h := &powerRentalOrderHandler{
		PowerRentalOrder: _powerRentalOrder,
		persistent:       persistent,
		notif:            notif,
		done:             done,
	}
	return h.exec(ctx)
}
