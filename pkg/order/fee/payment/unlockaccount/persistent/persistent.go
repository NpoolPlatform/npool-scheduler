package persistent

import (
	"context"
	"fmt"

	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	accountsvcname "github.com/NpoolPlatform/account-middleware/pkg/servicename"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/payment/unlockaccount/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	state := ordertypes.OrderState_OrderStatePaid
	rollback := true
	req := &feeordermwpb.FeeOrderReq{
		ID:         &order.ID,
		OrderState: &state,
		Rollback:   &rollback,
	}
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.fee.v1.Middleware/UpdateFeeOrder",
		"order.middleware.fee.v1.Middleware/UpdateFeeOrder",
		&feeordermwpb.UpdateFeeOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) withUnlockPaymentAccount(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	// TODO: use UpdateAccounts in future
	for _, id := range order.PaymentAccountIDs {
		locked := false
		req := &payaccmwpb.AccountReq{
			ID:     &id,
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

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
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
	})
	p.withUpdateOrderState(sagaDispose, _order)
	p.withUnlockPaymentAccount(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
