package persistent

import (
	"context"
	"fmt"

	achievementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/achievement/types"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, powerRentalOrder interface{}, notif, done chan interface{}) error {
	_powerRentalOrder, ok := powerRentalOrder.(*types.PersistentPowerRentalOrder)
	if !ok {
		return fmt.Errorf("invalid powerrentalorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _powerRentalOrder, done)

	if !_order.Simulate {
		if err := achievementmwcli.ExpropriateAchievement(ctx, _order.EntID); err != nil {
			return err
		}
	}

	state := ordertypes.OrderState_OrderStateReturnCanceledBalance
	if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
		ID:         &_order.ID,
		OrderState: &state,
	}); err != nil {
		return err
	}

	return nil
}
