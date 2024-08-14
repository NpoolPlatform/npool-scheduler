package persistent

import (
	"context"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderusersvcname "github.com/NpoolPlatform/miningpool-middleware/pkg/servicename"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/deleteproportion/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withSetProportion(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	for _, coinTypeID := range order.CoinTypeIDs {
		dispose.Add(
			orderusersvcname.ServiceDomain,
			"miningpool.middleware.orderuser.v1.Middleware/UpdateOrderUser",
			"",
			&orderusermwpb.UpdateOrderUserRequest{
				Info: &orderusermwpb.OrderUserReq{
					EntID:      order.PowerRentalOrder.PoolOrderUserID,
					CoinTypeID: &coinTypeID,
					Proportion: &order.Proportion,
				},
			},
		)
	}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *powerrentalordermwpb.PowerRentalOrder) {
	state := ordertypes.OrderState_OrderStateRestoreExpiredStock
	rollback := true
	req := &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &order.ID,
		OrderState: &state,
		Rollback:   &rollback,
	}
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		&powerrentalordermwpb.UpdatePowerRentalOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return wlog.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})

	p.withSetProportion(sagaDispose, _order)
	p.withUpdateOrderState(sagaDispose, _order.PowerRentalOrder)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
