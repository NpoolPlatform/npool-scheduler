package persistent

import (
	"context"
	"fmt"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/achievement/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, order interface{}, retry, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	orderState := ordertypes.OrderState_OrderStatePaid
	paymentState := ordertypes.PaymentState_PaymentStateDone
	reqs := []*ordermwpb.OrderReq{
		{
			ID:           &_order.ID,
			OrderState:   &orderState,
			PaymentState: &paymentState,
		},
	}
	for _, child := range _order.ChildOrders {
		reqs = append(reqs, &ordermwpb.OrderReq{
			ID:           &child.ID,
			OrderState:   &orderState,
			PaymentState: &paymentState,
		})
	}

	if _, err := ordermwcli.UpdateOrders(ctx, reqs); err != nil {
		retry1.Retry(ctx, _order, retry)
		return err
	}

	asyncfeed.AsyncFeed(_order, done)

	return nil
}
