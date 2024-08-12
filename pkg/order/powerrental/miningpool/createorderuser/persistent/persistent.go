package persistent

import (
	"context"
	"fmt"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderusersvcname "github.com/NpoolPlatform/miningpool-middleware/pkg/servicename"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/createorderuser/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withCreateOrderUser(dispose *dtmcli.SagaDispose, req *orderusermwpb.OrderUserReq) {
	dispose.Add(
		orderusersvcname.ServiceDomain,
		"miningpool.middleware.orderuser.v1.Middleware/CreateOrderUser",
		"miningpool.middleware.orderuser.v1.Middleware/DeleteOrderUser",
		&orderusermwpb.CreateOrderUserRequest{
			Info: req,
		},
	)
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *powerrentalordermwpb.PowerRentalOrderReq) {
	state := ordertypes.OrderState_OrderStateSetProportion
	rollback := true
	req := &powerrentalordermwpb.PowerRentalOrderReq{
		ID:              order.ID,
		OrderState:      &state,
		PoolOrderUserID: order.PoolOrderUserID,
		Rollback:        &rollback,
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
		return fmt.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})

	p.withCreateOrderUser(sagaDispose, _order.OrderUserReq)
	p.withUpdateOrderState(sagaDispose, _order.PowerRentalOrderReq)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
