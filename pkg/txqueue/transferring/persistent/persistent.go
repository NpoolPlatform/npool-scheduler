package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/transferring/types"
)

type handler struct {
	basepersistent.Persistent
}

func NewPersistent(ctx context.Context, cancel context.CancelFunc) basepersistent.Persistent {
	p := &handler{}
	p.Persistent = basepersistent.NewPersistent(ctx, cancel, p)
	return p
}

func (p *handler) Update(ctx context.Context, tx interface{}) error {
	_tx, ok := tx.(*types.PersistentTx)
	if !ok {
		return fmt.Errorf("invalid tx")
	}

	if _, err := txmwcli.UpdateTx(ctx, &txmwpb.TxReq{
		ID:        &_tx.ID,
		State:     &_tx.NewTxState,
		ChainTxID: &_tx.TxCID,
		Extra:     &_tx.TxExtra,
	}); err != nil {
		return err
	}

	return nil
}
