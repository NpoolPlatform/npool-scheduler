package persistent

import (
	"context"
	"fmt"

	goodsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	appstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/stock"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/restorestock/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateDeductLockedCommission
	rollback := true
	req := &ordermwpb.OrderReq{
		ID:         &order.ID,
		OrderState: &state,
		Rollback:   &rollback,
	}
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.order1.v1.Middleware/UpdateOrder",
		"order.middleware.order1.v1.Middleware/UpdateOrder",
		&ordermwpb.UpdateOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) withUpdateStock(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	switch order.CancelState {
	case ordertypes.OrderState_OrderStateWaitPayment:
		dispose.Add(
			goodsvcname.ServiceDomain,
			"good.middleware.app.good1.stock.v1.Middleware/Unlock",
			"",
			&appstockmwpb.UnlockRequest{
				LockID: order.AppGoodStockLockID,
			},
		)
	case ordertypes.OrderState_OrderStatePaid:
		fallthrough //nolint
	case ordertypes.OrderState_OrderStateInService:
		dispose.Add(
			goodsvcname.ServiceDomain,
			"good.middleware.app.good1.stock.v1.Middleware/ChargeBack",
			"",
			&appstockmwpb.ChargeBackRequest{
				LockID: order.AppGoodStockLockID,
			},
		)
	}
}

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
		RetryInterval:  timeoutSeconds,
	})
	p.withUpdateOrderState(sagaDispose, _order)
	p.withUpdateStock(sagaDispose, _order)
	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
