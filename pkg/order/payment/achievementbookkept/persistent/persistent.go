package persistent

import (
	"context"
	"fmt"

	goodsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	appstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/stock"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/stock/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, order interface{}, retry, notif chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	if _, err := achievementstatementmwcli.CreateStatements(ctx, h.AchievementStatements); err != nil {
		retry1.Retry(ctx, retry, _order)
		return err
	}
	if _, err := ledgerstatementmwcli.CreateStatements(ctx, h.LedgerStatements); err != nil {
		retry1.Retry(ctx, retry, _order)
		return err
	}
	state := ordertypes.OrderState_
	if _, err := ordermwcli.UpdateOrderState(ctx, &ordermwpb.OrderReq {
		ID: &_order.ID,

	})

	return nil
}
