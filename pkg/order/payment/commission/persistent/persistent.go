package persistent

import (
	"context"
	"fmt"

	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/commission/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateAchievementBookKeeping
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

func (p *handler) withCreateCommission(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if order.Simulate {
		return
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v2.Middleware/CreateStatements",
		"",
		&statementmwpb.CreateStatementsRequest{
			Infos: order.LedgerStatements,
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
	p.withCreateCommission(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
