package persistent

import (
	"context"
	"fmt"

	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	goodcoinrewardmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/coin/reward"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/done/types"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) updateOrders(ctx context.Context, good *types.PersistentGood) error {
	reqs := []*powerrentalordermwpb.PowerRentalOrderReq{}
	state := ordertypes.BenefitState_BenefitWait
	for _, id := range good.BenefitOrderIDs {
		_id := id
		reqs = append(reqs, &powerrentalordermwpb.PowerRentalOrderReq{
			ID:           &_id,
			BenefitState: &state,
		})
	}
	return powerrentalordermwcli.UpdatePowerRentalOrders(ctx, reqs)
}

func (p *handler) updateGood(ctx context.Context, good *types.PersistentGood) error {
	reqs := []*goodcoinrewardmwpb.RewardReq{}
	for _, reward := range good.CoinNextRewards {
		reqs = append(reqs, &goodcoinrewardmwpb.RewardReq{
			CoinTypeID:            &reward.CoinTypeID,
			NextRewardStartAmount: &reward.NextRewardStartAmount,
		})
	}
	state := goodtypes.BenefitState_BenefitWait
	return powerrentalmwcli.UpdatePowerRental(ctx, &powerrentalmwpb.PowerRentalReq{
		ID:          &good.ID,
		RewardState: &state,
		Rewards:     reqs,
	})
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGood)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)
	asyncfeed.AsyncFeed(ctx, _good, notif)

	if err := p.updateOrders(ctx, _good); err != nil {
		return err
	}
	return p.updateGood(ctx, _good)
}
