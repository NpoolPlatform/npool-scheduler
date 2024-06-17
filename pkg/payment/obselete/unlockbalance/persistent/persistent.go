package persistent

import (
	"context"
	"fmt"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/unlockbalance/types"
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

	// TODO: here state is not atomic
	if err := paymentmwcli.UpdatePayment(ctx, &paymentmwpb.PaymentReq{
		ID:            &_payment.ID,
		ObseleteState: ordertypes.PaymentObseleteState_PaymentObseleteTransferBookKeeping.Enum(),
	}); err != nil {
		return err
	}

	if _payment.XLedgerLockID != nil {
		if _, err := ledgermwcli.UnlockBalances(ctx, &ledgermwpb.UnlockBalancesRequest{
			LockID: *_payment.XLedgerLockID,
		}); err != nil {
			return err
		}
	}

	return nil
}
