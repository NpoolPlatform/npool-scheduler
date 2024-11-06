package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, order interface{}, persistent, notif, done chan interface{}) error {
	_order, ok := order.(*powerrentalordermwpb.PowerRentalOrder)
	if !ok {
		return wlog.Errorf("invalid order")
	}

	h := &orderHandler{
		PowerRentalOrder: _order,
		persistent:       persistent,
		done:             done,
		notif:            notif,
	}
	return h.exec(ctx)
}
