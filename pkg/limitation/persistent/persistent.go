package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/limitation/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, coin interface{}, reward, notif, done chan interface{}) error {
	_coin, ok := coin.(*types.PersistentCoin)
	if !ok {
		return fmt.Errorf("invalid coin")
	}

	defer asyncfeed.AsyncFeed(ctx, _coin, done)

	txType := basetypes.TxType_TxLimitation
	extra := fmt.Sprintf(`{"CoinName":"%v","CoinUnit":"%v","ENV":"%v","FromAddress":"%v","ToAddress":"%v"}`, _coin.Name, _coin.Unit, _coin.ENV, _coin.FromAddress, _coin.ToAddress)
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		CoinTypeID:    &_coin.EntID,
		FromAccountID: &_coin.FromAccountID,
		ToAccountID:   &_coin.ToAccountID,
		Amount:        &_coin.Amount,
		FeeAmount:     &_coin.FeeAmount,
		Extra:         &extra,
		Type:          &txType,
	}); err != nil {
		return err
	}

	return nil
}
