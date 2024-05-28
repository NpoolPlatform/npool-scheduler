package persistent

import (
	"context"

	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	achievementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/cancel/achievement/types"
	feeordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/fee"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentFeeOrder)
	if !ok {
		return wlog.Errorf("invalid feeorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	if err := achievementmwcli.ExpropriateAchievement(ctx, _order.EntID); err != nil {
		return wlog.WrapError(err)
	}

	return wlog.WrapError(
		feeordermwcli.UpdateFeeOrder(ctx, &feeordermwpb.FeeOrderReq{
			ID:         &_order.ID,
			OrderState: ordertypes.OrderState_OrderStateReturnCanceledBalance.Enum(),
		}),
	)
}
