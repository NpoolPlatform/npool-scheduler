package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/wait/types"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) updateOrders(ctx context.Context, good *types.PersistentPowerRental) error {
	reqs := []*powerrentalordermwpb.PowerRentalOrderReq{}
	state := ordertypes.BenefitState_BenefitCalculated
	for _, id := range good.BenefitOrderIDs {
		_id := id
		reqs = append(reqs, &powerrentalordermwpb.PowerRentalOrderReq{
			ID:            &_id,
			LastBenefitAt: &good.BenefitTimestamp,
			BenefitState:  &state,
		})
	}
	return powerrentalordermwcli.UpdatePowerRentalOrders(ctx, reqs)
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentPowerRental)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)

	if err := p.updateOrders(ctx, _good); err != nil {
		return err
	}

	id := uuid.Nil.String()
	if _good.Transferrable {
		id = uuid.NewString()
	}

	state := goodtypes.BenefitState_BenefitTransferring
	if err := powerrentalmwcli.UpdatePowerRental(ctx, &powerrentalmwpb.PowerRentalReq{
		ID:          &_good.ID,
		RewardAt:    &_good.BenefitTimestamp,
		RewardState: &state,
	}); err != nil {
		return err
	}

	if !_good.Transferrable {
		return nil
	}

	txType := basetypes.TxType_TxUserBenefit
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		EntID:         &id,
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
