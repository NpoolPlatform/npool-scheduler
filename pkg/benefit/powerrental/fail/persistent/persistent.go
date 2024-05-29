package persistent

import (
	"context"
	"fmt"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/fail/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) updateOrders(ctx context.Context, good *types.PersistentGood) error {
	reqs := []*ordermwpb.OrderReq{}
	state := ordertypes.BenefitState_BenefitWait
	for _, id := range good.BenefitOrderIDs {
		_id := id
		reqs = append(reqs, &ordermwpb.OrderReq{
			ID:           &_id,
			BenefitState: &state,
		})
	}
	if _, err := ordermwcli.UpdateOrders(ctx, reqs); err != nil {
		return err
	}
	return nil
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGood)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)

	if err := p.updateOrders(ctx, _good); err != nil {
		return err
	}

	state := goodtypes.BenefitState_BenefitWait
	if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:                    &_good.ID,
		RewardState:           &state,
		NextRewardStartAmount: &_good.NextStartRewardAmount,
	}); err != nil {
		return err
	}

	return nil
}
