package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) updateOrders(ctx context.Context, good *types.PersistentGood) error {
	reqs := []*ordermwpb.OrderReq{}
	state := ordertypes.BenefitState_BenefitCalculated
	for _, id := range good.BenefitOrderIDs {
		_id := id
		reqs = append(reqs, &ordermwpb.OrderReq{
			ID:            &_id,
			LastBenefitAt: &good.BenefitTimestamp,
			BenefitState:  &state,
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

	id := uuid.NewString()
	state := goodtypes.BenefitState_BenefitTransferring
	if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:                    &_good.ID,
		RewardTID:             &id,
		RewardAt:              &_good.BenefitTimestamp,
		RewardState:           &state,
		RewardAmount:          &_good.TodayRewardAmount,
		UnitRewardAmount:      &_good.TodayUnitRewardAmount,
		NextRewardStartAmount: &_good.NextStartRewardAmount,
	}); err != nil {
		return err
	}

	if !_good.Transferrable {
		return nil
	}

	txType := basetypes.TxType_TxUserBenefit
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		ID:            &id,
		CoinTypeID:    &_good.CoinTypeID,
		FromAccountID: &_good.GoodBenefitAccountID,
		ToAccountID:   &_good.UserBenefitHotAccountID,
		Amount:        &_good.TodayRewardAmount,
		FeeAmount:     &_good.FeeAmount,
		Extra:         &_good.Extra,
		Type:          &txType,
	}); err != nil {
		return err
	}

	return nil
}
