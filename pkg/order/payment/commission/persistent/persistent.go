package persistent

import (
	"context"
	"fmt"

	achievementstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/commission/types"
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

	if len(_order.AchievementStatements) > 0 {
		if _, err := achievementstatementmwcli.CreateStatements(ctx, _order.AchievementStatements); err != nil {
			retry1.Retry(ctx, _order, retry)
			return err
		}
	}
	state := ordertypes.OrderState_OrderStateAchievementBookKept
	if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
		ID:         &_order.ID,
		OrderState: &state,
	}); err != nil {
		retry1.Retry(ctx, _order, retry)
		return err
	}

	asyncfeed.AsyncFeed(ctx, _order, done)

	return nil
}
