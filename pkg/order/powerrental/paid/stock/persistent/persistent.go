package persistent

import (
	"context"
	"fmt"

	goodsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	appstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/stock"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateInService
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

func (p *handler) withUpdateStock(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	dispose.Add(
		goodsvcname.ServiceDomain,
		"good.middleware.app.good1.stock.v1.Middleware/InService",
		"",
		&appstockmwpb.InServiceRequest{
			LockID: order.AppGoodStockLockID,
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
	p.withUpdateOrderState(sagaDispose, _order)
	p.withUpdateStock(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
