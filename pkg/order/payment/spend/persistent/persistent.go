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
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/spend/types"
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
	state := ordertypes.OrderState_OrderStateTransferGoodStockLocked
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

func (p *handler) withSpendLockedBalance(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	balance := decimal.RequireFromString(order.OrderBalanceAmount)
	if balance.Cmp(decimal.NewFromInt(0)) <= 0 && len(order.Balances) == 0 {
		return
	}
	if order.MultiPaymentCoins {
		statementIDs := []string{}
		for range order.Balances {
			statementIDs = append(statementIDs, uuid.NewString())
		}
		dispose.Add(
			ledgersvcname.ServiceDomain,
			"ledger.middleware.ledger.v2.Middleware/SettleBalances",
			"",
			&ledgermwpb.SettleBalancesRequest{
				LockID:       order.OrderBalanceLockID,
				StatementIDs: statementIDs,
				IOExtra:      order.BalanceExtra,
				IOSubType:    ledgertypes.IOSubType_Payment,
			},
		)
	} else {
		dispose.Add(
			ledgersvcname.ServiceDomain,
			"ledger.middleware.ledger.v2.Middleware/SettleBalance",
			"",
			&ledgermwpb.SettleBalanceRequest{
				LockID:      order.OrderBalanceLockID,
				StatementID: uuid.NewString(),
				IOExtra:     order.BalanceExtra,
				IOSubType:   ledgertypes.IOSubType_Payment,
			},
		)
	}
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
	p.withSpendLockedBalance(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
