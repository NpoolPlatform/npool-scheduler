package persistent

import (
	"context"
	"fmt"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/created/types"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, order interface{}, reward, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentPowerRentalOrder)
	if !ok {
		return fmt.Errorf("invalid powerrentalorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	return powerrentalordermwcli.UpdatePowerRentalOrder(ctx, &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &_order.ID,
		OrderState: ordertypes.OrderState_OrderStateWaitPayment.Enum(),
	})
}
