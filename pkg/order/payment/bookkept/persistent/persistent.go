package persistent

import (
	"context"
	"fmt"

	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/bookkept/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStatePaymentBalanceSpent
	rollback := true
	req := &ordermwpb.OrderReq{
		ID:         &order.ID,
		OrderState: &state,
		Rollback:   &rollback,
	}
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.order1.v1.Middleare/UpdateOrder",
		"order.middleware.order1.v1.Middleare/UpdateOrder",
		&ordermwpb.UpdateOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) withSpendLockedBalance(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	balance := decimal.RequireFromString(order.BalanceAmount)
	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return
	}

	ioSubType := ledgertypes.IOSubType_Payment
	id := uuid.NewString()

	req := &ledgermwpb.LedgerReq{
		AppID:       &order.AppID,
		UserID:      &order.UserID,
		CoinTypeID:  &order.PaymentCoinTypeID,
		Locked:      &order.BalanceAmount,
		IOExtra:     &order.BalanceExtra,
		IOSubType:   &ioSubType,
		StatementID: &id,
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.v2.Middleware/SubBalance",
		"",
		&ledgermwpb.SubBalanceRequest{
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
	p.withSpendLockedBalance(sagaDispose, _order)
	p.withUpdateOrderState(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		retry1.Retry(ctx, _order, retry)
		return err
	}

	asyncfeed.AsyncFeed(_order, done)

	return nil
}
