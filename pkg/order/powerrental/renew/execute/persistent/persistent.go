package persistent

import (
	"context"
	"fmt"
	"time"

	ledgermwsvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	powerrentaloutofgasmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental/outofgas"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/execute/types"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"
	ordermwsvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withLockBalances(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	balances := func() (_balances []*ledgermwpb.LockBalancesRequest_XBalance) {
		for _, req := range order.FeeOrderReqs {
			for _, balance := range req.PaymentBalances {
				_balances = append(_balances, &ledgermwpb.LockBalancesRequest_XBalance{
					CoinTypeID: *balance.CoinTypeID,
					Amount:     *balance.Amount,
				})
			}
		}
		return
	}()
	dispose.Add(
		ledgermwsvcname.ServiceDomain,
		"ledger.middleware.ledger.v2.Middleware/LockBalances",
		"ledger.middleware.ledger.v2.Middleware/UnlockBalances",
		&ledgermwpb.LockBalancesRequest{
			AppID:    order.AppID,
			UserID:   order.UserID,
			LockID:   order.LedgerLockID,
			Rollback: true,
			Balances: balances,
		},
	)
}

func (p *handler) withCreateFeeOrders(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	dispose.Add(
		ordermwsvcname.ServiceDomain,
		"order.middleware.fee.v1.Middleware/CreateFeeOrders",
		"order.middleware.fee.v1.Middleware/DeleteFeeOrders",
		&feeordermwpb.CreateFeeOrdersRequest{
			Infos: order.FeeOrderReqs,
		},
	)
}

func (p *handler) withFinishOutOfGas(dispose *dtmcli.SagaDispose, order *types.PersistentOrder) {
	if order.OutOfGasEntID != nil {
		dispose.Add(
			ordermwsvcname.ServiceDomain,
			"order.middleware.powerrental.outofgas.v1.Middleware/UpdateOutOfGas",
			"",
			&powerrentaloutofgasmwpb.OutOfGasReq{
				EntID: order.OutOfGasEntID,
				EndAt: func() *uint32 { u := uint32(time.Now().Unix()); return &u }(),
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

	if err := powerrentalordermwcli.UpdatePowerRentalOrder(ctx, &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &_order.ID,
		RenewState: &_order.NewRenewState,
	}); err != nil {
		return err
	}
	if _order.InsufficientBalance {
		return nil
	}

	const timeoutSeconds = 30
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
	})
	p.withCreateFeeOrders(sagaDispose, _order)
	p.withLockBalances(sagaDispose, _order)
	p.withFinishOutOfGas(sagaDispose, _order)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
