package persistent

import (
	"context"
	"fmt"

	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/cancel/bookkeeping/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentFeeOrder) {
	req := &feeordermwpb.FeeOrderReq{
		ID:         &order.ID,
		OrderState: ordertypes.OrderState_OrderStateCancelUnlockPaymentAccount.Enum(),
		Rollback:   func() *bool { b := true; return &b }(),
		PaymentTransfers: func() (paymentTransfers []*paymentmwpb.PaymentTransferReq) {
			for _, paymentTransfer := range order.XPaymentTransfers {
				paymentTransfers = append(paymentTransfers, &paymentmwpb.PaymentTransferReq{
					EntID:        &paymentTransfer.PaymentTransferID,
					FinishAmount: &paymentTransfer.FinishAmount,
				})
			}
			return
		}(),
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

func (p *handler) withCreateIncomingStatements(dispose *dtmcli.SagaDispose, order *types.PersistentFeeOrder) {
	reqs := []*statementmwpb.StatementReq{}
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_Payment

	for _, paymentTransfer := range order.XPaymentTransfers {
		if paymentTransfer.IncomingAmount == nil {
			continue
		}
		reqs = append(reqs, &statementmwpb.StatementReq{
			AppID:      &order.AppID,
			UserID:     &order.UserID,
			CoinTypeID: &paymentTransfer.CoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     paymentTransfer.IncomingAmount,
			IOExtra:    &order.IncomingExtra,
		})
	}
	if len(reqs) == 0 {
		return
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v2.Middleware/CreateStatements",
		"",
		&statementmwpb.CreateStatementsRequest{
			Infos: reqs,
		},
	)
}

func (p *handler) Update(ctx context.Context, order interface{}, reward, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentFeeOrder)
	if !ok {
		return fmt.Errorf("invalid feeorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
		RetryInterval:  timeoutSeconds,
	})
	p.withUpdateOrderState(sagaDispose, _order)
	p.withCreateIncomingStatements(sagaDispose, _order)
	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
