package persistent

import (
	"context"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	fractionwithdrawalmwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/fractionwithdrawal"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderusersvcname "github.com/NpoolPlatform/miningpool-middleware/pkg/servicename"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/checkpoolbalance/types"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withCreateFractionWithdrawal(dispose *dtmcli.SagaDispose, reqs []*fractionwithdrawalmwpb.FractionWithdrawalReq) {
	for _, req := range reqs {
		dispose.Add(
			orderusersvcname.ServiceDomain,
			"miningpool.middleware.fractionwithdrawal.v1.Middleware/CreateFractionWithdrawal",
			"",
			&fractionwithdrawalmwpb.CreateFractionWithdrawalRequest{
				Info: req,
			},
		)
	}
}

func (p *handler) withUpdateOrder(dispose *dtmcli.SagaDispose, req *powerrentalordermwpb.PowerRentalOrderReq) {
	rollback := true
	req.Rollback = &rollback
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrder",
		&powerrentalordermwpb.UpdatePowerRentalOrderRequest{
			Info: req,
		},
	)
}

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return wlog.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})

	if len(_order.FractionWithdrawalReqs) > 0 {
		p.withCreateFractionWithdrawal(sagaDispose, _order.FractionWithdrawalReqs)
	}
	if _order.PowerRentalOrderReq != nil {
		p.withUpdateOrder(sagaDispose, _order.PowerRentalOrderReq)
	}

	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
