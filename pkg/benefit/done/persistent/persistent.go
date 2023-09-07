package persistent

import (
	"context"
	"fmt"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/done/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGood)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)
	asyncfeed.AsyncFeed(ctx, _good, notif)

	state := goodtypes.BenefitState_BenefitWait
	if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:          &_good.ID,
		RewardState: &state,
	}); err != nil {
		return err
	}

	return nil
}
