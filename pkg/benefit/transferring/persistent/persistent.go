package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/transferring/types"
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

	if _good.NewBenefitState == goodtypes.BenefitState_BenefitFail {
		asyncfeed.AsyncFeed(ctx, _good, notif)
	}

	if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:          &_good.ID,
		RewardState: &_good.NewBenefitState,
	}); err != nil {
		return err
	}

	if !_good.TransferToPlatform {
		return nil
	}

	txType := basetypes.TxType_TxPlatformBenefit
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		CoinTypeID:    &_good.CoinTypeID,
		FromAccountID: &_good.UserBenefitHotAccountID,
		ToAccountID:   &_good.PlatformColdAccountID,
		Amount:        &_good.ToPlatformAmount,
		FeeAmount:     &_good.FeeAmount,
		Extra:         &_good.Extra,
		Type:          &txType,
	}); err != nil {
		return err
	}

	return nil
}
