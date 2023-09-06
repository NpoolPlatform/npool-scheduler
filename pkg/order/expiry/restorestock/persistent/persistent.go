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
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/restorestock/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateExpired
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
	req := &appstockmwpb.StockReq{
		AppID:     &order.AppID,
		GoodID:    &order.GoodID,
		AppGoodID: &order.AppGoodID,
		InService: &order.Units,
	}
	dispose.Add(
		goodsvcname.ServiceDomain,
		"good.middleware.app.good1.stock.v1.Middleware/SubStock",
		"",
		&appstockmwpb.AddStockRequest{
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
	p.withUpdateStock(sagaDispose, _order)
	p.withUpdateOrderState(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
