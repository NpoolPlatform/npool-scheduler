package persistent

import (
	"context"
	"fmt"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/updatechilds/types"
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

	orderState := ordertypes.OrderState_OrderStateExpired
	reqs := []*ordermwpb.OrderReq{
		{
			ID:         &_order.ID,
			OrderState: &orderState,
		},
	}
	for _, child := range _order.ChildOrders {
		reqs = append(reqs, &ordermwpb.OrderReq{
			ID:         &child.ID,
			OrderState: &orderState,
		})
	}
	if _, err := ordermwcli.UpdateOrders(ctx, reqs); err != nil {
		return err
	}

	return nil
}
