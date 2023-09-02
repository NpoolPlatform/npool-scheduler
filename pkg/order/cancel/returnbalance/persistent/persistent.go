package persistent

import (
	"context"
	"fmt"

	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/returnbalance/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/shopspring/decimal"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateCanceled
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

func (p *handler) withReturnLockedBalance(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if order.LockedBalanceAmount == nil {
		return
	}

	balance := decimal.RequireFromString(*order.LockedBalanceAmount)
	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return
	}

	req := &ledgermwpb.LedgerReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		Spendable:  order.LockedBalanceAmount,
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.v2.Middleware/AddBalance",
		"",
		&ledgermwpb.SubBalanceRequest{
			Info: req,
		},
	)
}

func (p *handler) withReturnSpent(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if order.SpentAmount == nil {
		return
	}

	balance := decimal.RequireFromString(*order.SpentAmount)
	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return
	}

	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_OrderRevoke

	req := &statementmwpb.StatementReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     order.SpentAmount,
		IOExtra:    &order.SpentExtra,
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v2.Middleware/CreateStatement",
		"",
		&statementmwpb.CreateStatementRequest{
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
	p.withUpdateOrderState(sagaDispose, _order)
	p.withReturnLockedBalance(sagaDispose, _order)
	p.withReturnSpent(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		retry1.Retry(ctx, _order, retry)
		return err
	}

	asyncfeed.AsyncFeed(_order, done)

	return nil
}
