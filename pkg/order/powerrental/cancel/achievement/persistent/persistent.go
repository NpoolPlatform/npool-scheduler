package persistent

import (
	"context"

	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
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
		return wlog.Errorf("invalid powerrentalorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _powerRentalOrder, done)

	if err := achievementmwcli.ExpropriateAchievement(ctx, _powerRentalOrder.EntID); err != nil {
		return wlog.WrapError(err)
	}

	return wlog.WrapError(
		powerrentalordermwcli.UpdatePowerRentalOrder(ctx, &powerrentalordermwpb.PowerRentalOrderReq{
			ID:         &_powerRentalOrder.ID,
			OrderState: ordertypes.OrderState_OrderStateReturnCanceledBalance.Enum(),
		}),
	)
}
