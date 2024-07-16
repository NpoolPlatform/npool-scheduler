package persistent

import (
	"context"
	"fmt"

	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/cancel/commission/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderState(dispose *dtmcli.SagaDispose, order *types.PersistentFeeOrder) {
	state := ordertypes.OrderState_OrderStateCancelAchievement
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

func (p *handler) withDeductLockedCommission(dispose *dtmcli.SagaDispose, order *types.PersistentFeeOrder) {
	for _, revoke := range order.CommissionRevokes {
		dispose.Add(
			ledgersvcname.ServiceDomain,
			"ledger.middleware.ledger.v2.Middleware/SettleBalances",
			"",
			&ledgermwpb.SettleBalancesRequest{
				LockID:       revoke.LockID,
				IOSubType:    ledgertypes.IOSubType_CommissionRevoke,
				IOExtra:      revoke.IOExtra,
				StatementIDs: revoke.StatementIDs,
			},
		)
	}
}

func (p *handler) Update(ctx context.Context, order interface{}, reward, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentFeeOrder)
	if !ok {
		return fmt.Errorf("invalid order")
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
	p.withDeductLockedCommission(sagaDispose, _order)
	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
