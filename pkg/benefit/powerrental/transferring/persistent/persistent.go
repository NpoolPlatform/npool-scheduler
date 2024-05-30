package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/transferring/types"

	"github.com/shopspring/decimal"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentPowerRental)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)

	if err := powerrentalmwcli.UpdatePowerRental(ctx, &powerrentalmwpb.PowerRentalReq{
		ID:          &_good.ID,
		RewardState: &_good.NewBenefitState,
	}); err != nil {
		return err
	}

	if len(_good.CoinRewards) == 0 {
		return nil
	}

	txReqs := []*txmwpb.TxReq{}
	for _, reward := range _good.CoinRewards {
		txReqs = append(txReqs, &txmwpb.TxReq{
			CoinTypeID:    &reward.CoinTypeID,
			FromAccountID: &reward.UserBenefitHotAccountID,
			ToAccountID:   &reward.PlatformColdAccountID,
			Amount:        &reward.ToPlatformAmount,
			FeeAmount:     func() *string { s := decimal.NewFromInt(0).String(); return &s }(),
			Extra:         &reward.Extra,
			Type:          func() *basetypes.TxType { e := basetypes.TxType_TxPlatformBenefit; return &e }(),
		})
	}

	if _, err := txmwcli.CreateTxs(ctx, txReqs); err != nil {
		return err
	}

	return nil
}
