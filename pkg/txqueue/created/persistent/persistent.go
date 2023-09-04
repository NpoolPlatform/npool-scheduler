package persistent

import (
	"context"
	"fmt"
	"sync"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/types"
)

type handler struct {
	mutex *sync.Mutex
}

func NewPersistent(mutex *sync.Mutex) basepersistent.Persistenter {
	return &handler{
		mutex: mutex,
	}
}

func (p *handler) Update(ctx context.Context, tx interface{}, retry, notif, done chan interface{}) error {
	_tx, ok := tx.(*types.PersistentTx)
	if !ok {
		return fmt.Errorf("invalid tx")
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	state := basetypes.TxState_TxStateWait
	if _, err := txmwcli.UpdateTx(ctx, &txmwpb.TxReq{
		ID:    &_tx.ID,
		State: &state,
	}); err != nil {
		retry1.Retry(ctx, _tx, retry)
		return err
	}

	cancelablefeed.CancelableFeed(ctx, _tx, done)

	return nil
}
