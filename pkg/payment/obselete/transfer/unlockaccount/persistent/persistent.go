package persistent

import (
	"context"
	"fmt"

	paymentaccountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/transfer/unlockaccount/types"
	paymentmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/payment"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, payment interface{}, notif, done chan interface{}) error {
	_payment, ok := payment.(*types.PersistentPayment)
	if !ok {
		return fmt.Errorf("invalid payment")
	}

	defer asyncfeed.AsyncFeed(ctx, _payment, done)

	// Every time we update one account, until all accounts are unlocked
	if _payment.UnlockAccountID != nil {
		if _, err := paymentaccountmwcli.UnlockAccount(ctx, *_payment.UnlockAccountID); err != nil {
			return err
		}
		return nil
	}

	return paymentmwcli.UpdatePayment(ctx, &paymentmwpb.PaymentReq{
		ID:            &_payment.ID,
		ObseleteState: ordertypes.PaymentObseleteState_PaymentObseleted.Enum(),
	})
}
