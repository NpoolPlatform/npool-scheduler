package persistent

import (
	"context"
	"fmt"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodstmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/good/ledger/statement"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	goodstmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/good/ledger/statement"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/good/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) updateGood(ctx context.Context, good *types.PersistentGood) error {
	state := goodtypes.BenefitState_BenefitUserBookKeeping
	if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:          &good.ID,
		RewardState: &state,
	}); err != nil {
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

	if _good.StatementExist {
		if err := p.updateGood(ctx, _good); err != nil {
			return err
		}
		return nil
	}

	if _, err := goodstmwcli.CreateGoodStatement(ctx, &goodstmwpb.GoodStatementReq{
		GoodID:                    &_good.ID,
		CoinTypeID:                &_good.CoinTypeID,
		TotalAmount:               &_good.TotalRewardAmount,
		UnsoldAmount:              &_good.UnsoldRewardAmount,
		TechniqueServiceFeeAmount: &_good.TechniqueFeeAmount,
		BenefitDate:               &_good.LastRewardAt,
	}); err != nil {
		return err
	}
	if err := p.updateGood(ctx, _good); err != nil {
		return err
	}
	return nil
}
