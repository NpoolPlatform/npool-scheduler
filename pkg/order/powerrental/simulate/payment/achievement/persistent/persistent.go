package persistent

import (
	"context"
	"fmt"

	achievementstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/achievement/types"
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

	if !_order.Simulate {
		if len(_order.AchievementStatements) > 0 {
			if _, err := achievementstatementmwcli.CreateStatements(ctx, _order.AchievementStatements); err != nil {
				return err
			}
		}
	}
	state := ordertypes.OrderState_OrderStatePaymentUnlockAccount
	if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
		ID:         &_order.ID,
		OrderState: &state,
	}); err != nil {
		return err
	}

	return nil
}
