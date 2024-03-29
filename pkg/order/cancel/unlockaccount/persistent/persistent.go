package persistent

import (
	"context"
	"fmt"

	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	accountsvcname "github.com/NpoolPlatform/account-middleware/pkg/servicename"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/unlockaccount/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateUpdateCanceledChilds
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

func (p *handler) withUnlockPaymentAccount(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if order.Simulate {
		return
	}
	if order.OrderPaymentAccountID == nil {
		return
	}

	locked := false
	req := &payaccmwpb.AccountReq{
		ID:     order.OrderPaymentAccountID,
		Locked: &locked,
	}

	dispose.Add(
		accountsvcname.ServiceDomain,
		"account.middleware.payment.v1.Middleware/UpdateAccount",
		"",
		&payaccmwpb.UpdateAccountRequest{
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

	if err := accountlock.Lock(_order.PaymentAccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(_order.PaymentAccountID) //nolint
	}()

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
		RetryInterval:  timeoutSeconds,
	})
	p.withUpdateOrderState(sagaDispose, _order)
	p.withUnlockPaymentAccount(sagaDispose, _order)
	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
