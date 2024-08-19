package persistent

import (
	"context"
	"fmt"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	goodsvcname "github.com/NpoolPlatform/good-middleware/pkg/servicename"
	v1 "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	goodpowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/powerrental/checkhashrate/types"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdatePowerrentalState(dispose *dtmcli.SagaDispose, good *types.PersistentGoodPowerRental) {
	req := &goodpowerrentalmwpb.PowerRentalReq{
		ID:               &good.ID,
		EntID:            &good.EntID,
		GoodID:           &good.GoodID,
		State:            v1.GoodState_GoodStateCheckHashRate.Enum(),
		MiningGoodStocks: good.MiningGoodStockReqs,
		Rollback:         func() *bool { rollback := true; return &rollback }(),
	}

	dispose.Add(
		goodsvcname.ServiceDomain,
		"good.middleware.powerrental.v1.Middleware/UpdatePowerRental",
		"good.middleware.powerrental.v1.Middleware/UpdatePowerRental",
		&goodpowerrentalmwpb.UpdatePowerRentalRequest{
			Info: req,
		},
	)
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGoodPowerRental)
	if !ok {
		return fmt.Errorf("invalid feeorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)
	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
		RetryInterval:  timeoutSeconds,
	})

	p.withUpdatePowerrentalState(sagaDispose, _good)
	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}
	return nil
}
