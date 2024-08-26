package persistent

import (
	"context"
	"fmt"

	ledgerstatementmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger/statement"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/transfer/bookkeeping/types"
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

	if len(_payment.Statements) > 0 {
		if _, err := ledgerstatementmwcli.CreateStatements(ctx, _payment.Statements); err != nil {
			return err
		}
	}

	return paymentmwcli.UpdatePayment(ctx, &paymentmwpb.PaymentReq{
		ID:               &_payment.ID,
		ObseleteState:    ordertypes.PaymentObseleteState_PaymentObseleteTransferUnlockAccount.Enum(),
		PaymentTransfers: _payment.PaymentTransfers,
	})
}
