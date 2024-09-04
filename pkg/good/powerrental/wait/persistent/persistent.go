package persistent

import (
	"context"
	"fmt"

	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	v1 "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	goodpowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"

	"github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/powerrental/wait/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGoodPowerRental)
	if !ok {
		return fmt.Errorf("invalid feeorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)

	return powerrentalmwcli.CreatePowerRental(ctx, &goodpowerrentalmwpb.PowerRentalReq{
		ID:               &_good.ID,
		EntID:            &_good.EntID,
		GoodID:           &_good.GoodID,
		State:            v1.GoodState_GoodStateCreateGoodUser.Enum(),
		MiningGoodStocks: _good.MiningGoodStockReqs,
		Rollback:         func() *bool { rollback := true; return &rollback }(),
	})
}
