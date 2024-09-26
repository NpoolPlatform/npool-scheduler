package persistent

import (
	"context"
	"fmt"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/payment/timeout/types"
	feeordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/fee"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, order interface{}, reward, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	orderState := ordertypes.OrderState_OrderStatePreCancel
	return feeordermwcli.UpdateFeeOrder(ctx, &feeordermwpb.FeeOrderReq{
		ID:         &_order.ID,
		OrderState: &orderState,
	})
}
