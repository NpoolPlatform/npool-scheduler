package persistent

import (
	"context"
	"fmt"

	goodmwsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	ledgermwsvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	appgoodstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/stock"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/execute/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
	ordermwsvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withLockBalances(dispose *dtmcli.SagaDispose, order *types.PersistentOrder, balances []*ledgermwpb.LockBalancesRequest_XBalance, lockID string) {
	dispose.Add(
		ledgermwsvcname.ServiceDomain,
		"ledger.middleware.ledger.v2.Middleware/LockBalances",
		"ledger.middleware.ledger.v2.Middleware/UnlockBalances",
		&ledgermwpb.LockBalancesRequest{
			AppID:    order.AppID,
			UserID:   order.UserID,
			LockID:   lockID,
			Rollback: true,
			Balances: balances,
		},
	)
}

func (p *handler) withLockStock(dispose *dtmcli.SagaDispose, order *types.PersistentOrder, stock *appgoodstockmwpb.LocksRequest_XStock, lockID string) {
	dispose.Add(
		goodmwsvcname.ServiceDomain,
		"good.middleware.app.good1.stock.v1.Middleware/Lock",
		"good.middleware.app.good1.stock.v1.Middleware/Unlock",
		&appgoodstockmwpb.LockRequest{
			AppID:        order.AppID,
			EntID:        stock.EntID,
			GoodID:       stock.GoodID,
			AppGoodID:    stock.AppGoodID,
			Units:        stock.Units,
			AppSpotUnits: decimal.NewFromInt(0).String(),
			LockID:       lockID,
			Rollback:     true,
		},
	)
}

func (p *handler) withCreateOrder(dispose *dtmcli.SagaDispose, orderReq *ordermwpb.OrderReq) {
	dispose.Add(
		ordermwsvcname.ServiceDomain,
		"order.middleware.order1.v1.Middleware/CreateOrder",
		"order.middleware.order1.v1.Middleware/DeleteOrder",
		&ordermwpb.CreateOrderRequest{
			Info: orderReq,
		},
	)
}

func (p *handler) createOrder(dispose *dtmcli.SagaDispose, order *types.PersistentOrder, orderReq *types.OrderReq) {
	ledgerLockID := uuid.NewString()
	appGoodStockLockID := uuid.NewString()
	orderID := uuid.NewString()

	p.withLockBalances(dispose, order, orderReq.Balances, ledgerLockID)
	p.withLockStock(dispose, order, orderReq.Stock, appGoodStockLockID)

	orderReq.OrderReq.EntID = &orderID
	orderReq.OrderReq.LedgerLockID = &ledgerLockID
	orderReq.OrderReq.AppGoodStockLockID = &appGoodStockLockID
	p.withCreateOrder(dispose, orderReq.OrderReq)
}

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
		ID:         &_order.ID,
		RenewState: &_order.NewRenewState,
	}); err != nil {
		return err
	}
	if _order.InsufficientBalance {
		return nil
	}

	const timeoutSeconds = 30
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
	})

	for _, orderReq := range _order.OrderReqs {
		p.createOrder(sagaDispose, _order, orderReq)
	}
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
