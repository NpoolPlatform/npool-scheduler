package persistent

import (
	"context"
	"fmt"

	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	goodstatementmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/good/ledger/statement"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	goodstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/good/ledger/statement"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/bookkeeping/good/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) updateGood(ctx context.Context, good *types.PersistentGood) error {
	return powerrentalmwcli.UpdatePowerRental(ctx, &powerrentalmwpb.PowerRentalReq{
		ID:          &good.ID,
		RewardState: func() *goodtypes.BenefitState { e := goodtypes.BenefitState_BenefitUserBookKeeping; return &e }(),
	})
}

func (p *handler) createGoodStatements(ctx context.Context, good *types.PersistentGood) error {
	stReqs := []*goodstatementmwpb.GoodStatementReq{}
	for _, reward := range good.CoinRewards {
		stReqs = append(stReqs, &goodstatementmwpb.GoodStatementReq{
			GoodID:                    &good.GoodID,
			CoinTypeID:                &reward.CoinTypeID,
			TotalAmount:               &reward.TotalRewardAmount,
			UnsoldAmount:              &reward.UnsoldRewardAmount,
			TechniqueServiceFeeAmount: &reward.TechniqueFeeAmount,
			BenefitDate:               &good.LastRewardAt,
		})
	}
	if _, err := goodstatementmwcli.CreateGoodStatements(ctx, stReqs); err != nil {
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

	if len(_good.CoinRewards) > 0 {
		if err := p.createGoodStatements(ctx, _good); err != nil {
			return err
		}
	}
	return p.updateGood(ctx, _good)
}
