package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, coin interface{}, retry chan interface{}) error {
	_coin, ok := coin.(*types.PersistentCoin)
	if !ok {
		return fmt.Errorf("invalid coin")
	}

	txType := basetypes.TxType_TxFeedGas
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		CoinTypeID:    &_coin.FeeCoinTypeID,
		FromAccountID: &_coin.FromAccountID,
		ToAccountID:   &_coin.ToAccountID,
		Amount:        &_coin.Amount,
		FeeAmount:     &_coin.FeeAmount,
		Extra:         &_coin.Extra,
		Type:          &txType,
	}); err != nil {
		fmt.Printf("tx - %v - %v\n", _coin, err)
		return err
	}
	return nil
}
