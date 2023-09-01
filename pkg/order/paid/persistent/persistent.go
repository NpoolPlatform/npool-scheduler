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
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/paid/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateStock(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	rollback := true
	req := &appstockmwpb.StockReq{
		AppID:     &order.AppID,
		GoodID:    &order.GoodID,
		AppGoodID: &order.AppGoodID,
		InService: &order.Units,
		Rollback:  &rollback,
	}

	dispose.Add(
		goodsvcname.ServiceDomain,
		"good.middleware.app.good1.stock.v1.Middleware/AddStock",
		"good.middleware.app.good1.stock.v1.Middleware/SubStock",
		&appstockmwpb.AddStockRequest{
			Info: req,
		},
	)
}

func (p *handler) withUpdateOrder(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateInService
	req := &ordermwpb.OrderReq{
		ID:         &order.ID,
		OrderState: &state,
	}

	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.order.v1.Middleware/UpdateOrder",
		"",
		&ordermwpb.UpdateOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) Update(ctx context.Context, order interface{}, retry, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})
	p.withUpdateStock(sagaDispose, _order)
	p.withUpdateOrder(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		retry1.Retry(ctx, _order, retry)
		return err
	}

	asyncfeed.AsyncFeed(_order, done)

	return nil
}
