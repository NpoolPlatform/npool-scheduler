package persistent

import (
	"context"
	"fmt"

	goodsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	inspiresvcname "github.com/NpoolPlatform/inspire-middleware/pkg/servicename"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	appstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/stock"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
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
		Rollback:  &rollback,
	}
	switch order.NewOrderState {
	case ordertypes.OrderState_OrderStatePaid:
		req.WaitStart = &order.Units
		dispose.Add(
			goodsvcname.ServiceDomain,
			"good.middleware.app.good1.stock.v1.Middleware/AddStock",
			"good.middleware.app.good1.stock.v1.Middleware/SubStock",
			&appstockmwpb.AddStockRequest{
				Info: req,
			},
		)
	case ordertypes.OrderState_OrderStateCanceled:
		fallthrough //nolint
	case ordertypes.OrderState_OrderStatePaymentTimeout:
		req.Locked = &order.Units
		dispose.Add(
			goodsvcname.ServiceDomain,
			"good.middleware.app.good1.stock.v1.Middleware/SubStock",
			"good.middleware.app.good1.stock.v1.Middleware/AddStock",
			&appstockmwpb.AddStockRequest{
				Info: req,
			},
		)
	}
}

func (p *handler) withUpdateOrder(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	req := &ordermwpb.OrderReq{
		ID:         &order.ID,
		OrderState: &order.NewOrderState,
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

func (p *handler) withCreateIncomingStatement(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if order.IncomingAmount == nil {
		return
	}

	id := uuid.NewString()
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_Payment
	req := &ledgerstatementmwpb.StatementReq{
		ID:         &id,
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     order.IncomingAmount,
		IOExtra:    &order.IncomingExtra,
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v2.Middleware/CreateStatement",
		"ledger.middleware.ledger.statement.v2.Middleware/DeleteStatement",
		&ledgerstatementmwpb.CreateStatementRequest{
			Info: req,
		},
	)
}

func (p *handler) withCreateOutcomingStatement(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if order.NewOrderState != ordertypes.OrderState_OrderStatePaid {
		return
	}

	id := uuid.NewString()
	ioType := ledgertypes.IOType_Outcoming
	ioSubType := ledgertypes.IOSubType_Payment
	req := &ledgerstatementmwpb.StatementReq{
		ID:         &id,
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &order.TransferAmount,
		IOExtra:    &order.TransferOutcomingExtra,
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v2.Middleware/CreateStatement",
		"ledger.middleware.ledger.statement.v2.Middleware/DeleteStatement",
		&ledgerstatementmwpb.CreateStatementRequest{
			Info: req,
		},
	)
}

func (p *handler) withSpendLockedBalance(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	id := uuid.NewString()
	ioSubType := ledgertypes.IOSubType_Payment
	req := &ledgermwpb.LedgerReq{
		AppID:       &order.AppID,
		UserID:      &order.UserID,
		CoinTypeID:  &order.PaymentCoinTypeID,
		Locked:      &order.BalanceAmount,
		IOSubType:   &ioSubType,
		IOExtra:     &order.BalanceOutcomingExtra,
		StatementID: &id,
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.v2.Middleware/SubBalance",
		"ledger.middleware.ledger.v2.Middleware/AddBalance",
		&ledgermwpb.SubBalanceRequest{
			Info: req,
		},
	)
}

func (p *handler) withUnlockLockedBalance(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	id := uuid.NewString()
	ioSubType := ledgertypes.IOSubType_Payment
	req := &ledgermwpb.LedgerReq{
		AppID:       &order.AppID,
		UserID:      &order.UserID,
		CoinTypeID:  &order.PaymentCoinTypeID,
		Spendable:   &order.BalanceAmount,
		IOSubType:   &ioSubType,
		IOExtra:     &order.BalanceOutcomingExtra,
		StatementID: &id,
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.v2.Middleware/AddBalance",
		"ledger.middleware.ledger.v2.Middleware/SubBalance",
		&ledgermwpb.SubBalanceRequest{
			Info: req,
		},
	)
}

func (p *handler) withCreateAchievementStatements(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if len(order.AchievementStatements) == 0 {
		return
	}
	dispose.Add(
		inspiresvcname.ServiceDomain,
		"inspire.middleware.achievement.statement.v1.Middleware/CreateStatements",
		"inspire.middleware.achievement.statement.v1.Middleware/DeleteStatements",
		&achievementstatementmwpb.CreateStatementsRequest{
			Infos: order.AchievementStatements,
		},
	)
}

func (p *handler) withCreateCommissionStatements(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if len(order.CommissionStatements) == 0 {
		return
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v1.CreateStatements",
		"ledger.middleware.ledger.statement.v1.DeleteStatements",
		&ledgerstatementmwpb.CreateStatementsRequest{
			Infos: order.CommissionStatements,
		},
	)
}

func (p *handler) Update(ctx context.Context, order interface{}, retry, notif chan interface{}) error {
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
	p.withCreateIncomingStatement(sagaDispose, _order)
	p.withCreateOutcomingStatement(sagaDispose, _order)
	if _order.NewOrderState == ordertypes.OrderState_OrderStatePaid {
		p.withSpendLockedBalance(sagaDispose, _order)
	} else {
		p.withUnlockLockedBalance(sagaDispose, _order)
	}
	p.withCreateAchievementStatements(sagaDispose, _order)
	p.withCreateCommissionStatements(sagaDispose, _order)
	p.withUpdateOrder(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		retry1.Retry(ctx, _order, retry)
		return err
	}

	// Allocate reward of user purchase action
	// Send order payment notification or timeout hint

	return nil
}
