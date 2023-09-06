package persistent

import (
	"context"
	"fmt"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/check/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	req := &ordermwpb.OrderReq{
		ID:           &_order.ID,
		OrderState:   &_order.NewOrderState,
		PaymentState: &_order.NewPaymentState,
	}
	if _order.NewCancelState != nil {
		req.CancelState = _order.NewCancelState
	}

	if _, err := ordermwcli.UpdateOrder(ctx, req); err != nil {
		return err
	}

	return nil
}
