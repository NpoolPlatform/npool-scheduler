package persistent

import (
	"context"
	"fmt"

	pledgemwcli "github.com/NpoolPlatform/good-middleware/pkg/client/pledge"
	v1 "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	goodpledgemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/pledge"

	"github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/pledge/wait/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, good interface{}, reward, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGoodPledge)
	if !ok {
		return fmt.Errorf("invalid feeorder")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)

	return pledgemwcli.UpdatePledge(ctx, &goodpledgemwpb.PledgeReq{
		ID:            &_good.ID,
		EntID:         &_good.EntID,
		GoodID:        &_good.GoodID,
		ContractState: v1.ContractState_ContractInDeployment.Enum(),
		Rollback:      func() *bool { rollback := true; return &rollback }(),
	})
}
