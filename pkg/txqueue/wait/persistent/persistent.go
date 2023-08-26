package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/wait/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, tx interface{}, retry chan interface{}) error {
	_tx, ok := tx.(*types.PersistentTx)
	if !ok {
		return fmt.Errorf("invalid tx")
	}

	if !_tx.TransactionExist {
		if err := sphinxproxycli.CreateTransaction(ctx, &sphinxproxypb.CreateTransactionRequest{
			TransactionID: _tx.ID,
			Name:          _tx.CoinName,
			Amount:        _tx.FloatAmount,
			From:          _tx.FromAddress,
			Memo:          _tx.AccountMemo,
			To:            _tx.ToAddress,
		}); err != nil {
			retry1.Retry(ctx, _tx, retry)
			return err
		}
	}

	_tx.TransactionExist = true

	state := basetypes.TxState_TxStateTransferring
	if _, err := txmwcli.UpdateTx(ctx, &txmwpb.TxReq{
		ID:    &_tx.ID,
		State: &state,
	}); err != nil {
		retry1.Retry(ctx, _tx, retry)
		return err
	}

	return nil
}
