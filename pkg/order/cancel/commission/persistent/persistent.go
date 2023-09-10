package persistent

import (
	"context"
	"fmt"

	goodsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/commission/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateReturnCanceledBalance
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

func (p *handler) withDeductLockedCommission(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) error {
	if len(order.LedgerStatements) == 0 {
		return nil
	}
	for _, statement := range order.LedgerStatements {
		lock, ok := order.CommissionLocks[*statement.UserID]
		if !ok {
			return fmt.Errorf("invalid commission lock")
		}
		req := &ledgermwpb.LedgerReq{
			AppID:       statement.AppID,
			UserID:      statement.UserID,
			CoinTypeID:  statement.CoinTypeID,
			Locked:      statement.Amount,
			LockID:      &lock.ID,
			StatementID: statement.ID,
			IOSubType:   statement.IOSubType,
			IOExtra:     statement.IOExtra,
		}
		dispose.Add(
			goodsvcname.ServiceDomain,
			"ledger.middleware.ledger.v2.Middleware/SubBalance",
			"ledger.middleware.ledger.v2.Middleware/AddBalance",
			&ledgermwpb.SubBalanceRequest{
				Info: req,
			},
		)
	}
	return nil
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
	if err := p.withDeductLockedCommission(sagaDispose, _order); err != nil {
		return err
	}
	p.withUpdateOrderState(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
