package persistent

import (
	"context"
	"fmt"

	orderstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement/order"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/achievement/types"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"
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

	if len(_order.OrderStatements) > 0 {
		if err := orderstatementmwcli.CreateStatements(ctx, _order.OrderStatements); err != nil {
			return err
		}
	}
	state := ordertypes.OrderState_OrderStateAddCommission
	return powerrentalordermwcli.UpdatePowerRentalOrder(ctx, &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &_order.ID,
		OrderState: &state,
	})
}
