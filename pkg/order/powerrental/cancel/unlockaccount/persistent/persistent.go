package persistent

import (
	"context"
	"fmt"

	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	accountsvcname "github.com/NpoolPlatform/account-middleware/pkg/servicename"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/unlockaccount/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStateCanceled
	rollback := true
	req := &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &order.ID,
		OrderState: &state,
		Rollback:   &rollback,
	}
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		&powerrentalordermwpb.UpdatePowerRentalOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) withUnlockPaymentAccount(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	// TODO: use UpdateAccounts in future
	for _, paymentAccountID := range order.PaymentAccountIDs {
		locked := false
		req := &payaccmwpb.AccountReq{
			ID:     &paymentAccountID,
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
}

func (p *handler) lockPaymentTransferAccounts(order *types.PersistentOrder) error {
	for _, paymentTransfer := range order.PaymentTransfers {
		if err := accountlock.Lock(paymentTransfer.AccountID); err != nil {
			return wlog.WrapError(err)
		}
	}
	return nil
}

func (p *handler) unlockPaymentTransferAccounts(order *types.PersistentOrder) {
	for _, paymentTransfer := range order.PaymentTransfers {
		_ = accountlock.Unlock(paymentTransfer.AccountID) //nolint
	}
}

func (p *handler) Update(ctx context.Context, order interface{}, reward, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	if err := p.lockPaymentTransferAccounts(_order); err != nil {
		return err
	}
	defer p.unlockPaymentTransferAccounts(_order)

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
