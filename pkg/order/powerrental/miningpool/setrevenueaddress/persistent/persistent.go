package persistent

import (
	"context"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	goodsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	appstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/stock"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderusersvcname "github.com/NpoolPlatform/miningpool-middleware/pkg/servicename"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/setrevenueaddress/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withSetRevenueAddress(dispose *dtmcli.SagaDispose, reqs []*orderusermwpb.OrderUserReq) {
	for _, req := range reqs {
		dispose.Add(
			orderusersvcname.ServiceDomain,
			"miningpool.middleware.orderuser.v1.Middleware/UpdateOrderUser",
			"",
			&orderusermwpb.UpdateOrderUserRequest{
				Info: req,
			},
		)
	}
}

func (p *handler) withUpdateStock(dispose *dtmcli.SagaDispose, appGoodStockLockID string) {
	dispose.Add(
		goodsvcname.ServiceDomain,
		"good.middleware.app.good1.stock.v1.Middleware/InService",
		"",
		&appstockmwpb.InServiceRequest{
			LockID: appGoodStockLockID,
		},
	)
}

func (p *handler) withUpdateOrder(dispose *dtmcli.SagaDispose, req *powerrentalordermwpb.PowerRentalOrderReq) {
	rollback := true
	req.Rollback = &rollback

	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		&powerrentalordermwpb.UpdatePowerRentalOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) Update(ctx context.Context, order interface{}, reward, notif, done chan interface{}) error {
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

	if len(_order.OrderUserReqs) > 0 {
		p.withSetRevenueAddress(sagaDispose, _order.OrderUserReqs)
	}
	if _order.AppGoodStockLockID != nil {
		p.withUpdateStock(sagaDispose, *_order.AppGoodStockLockID)
	}
	if _order.PowerRentalOrderReq != nil {
		p.withUpdateOrder(sagaDispose, _order.PowerRentalOrderReq)
	}

	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
