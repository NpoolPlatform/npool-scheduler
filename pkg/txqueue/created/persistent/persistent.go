package persistent

import (
	"context"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/types"
)

type Persistent interface {
	Feed(*types.PersistentTx)
	Finalize()
}

type persistent struct {
	feeder chan *types.PersistentTx
	w      *watcher.Watcher
}

func NewPersistent(ctx context.Context, cancel context.CancelFunc) Persistent {
	p := &persistent{
		feeder: make(chan *types.PersistentTx),
		w:      watcher.NewWatcher(),
	}

	go action.Watch(ctx, cancel, p.run)
	return p
}

func (p *persistent) persistentTx(ctx context.Context, tx *types.PersistentTx) error {
	state := basetypes.TxState_TxStateWait
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		ID:    &tx.ID,
		State: &state,
	}); err != nil {
		return err
	}
	return nil
}

func (p *persistent) handler(ctx context.Context) bool {
	select {
	case tx := <-p.feeder:
		if err := p.persistentTx(ctx, tx); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "persistentTx",
				"Error", err,
			)
		}
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"handler",
			"State", "Done",
			"Error", ctx.Err(),
		)
		close(p.w.ClosedChan())
		return true
	case <-p.w.CloseChan():
		close(p.w.ClosedChan())
		return true
	}
}

func (p *persistent) run(ctx context.Context) {
	for {
		if b := p.handler(ctx); b {
			break
		}
	}
}

func (p *persistent) Finalize() {
	if p != nil && p.w != nil {
		p.w.Shutdown()
		close(p.feeder)
	}
}

func (p *persistent) Feed(tx *types.PersistentTx) {
	p.feeder <- tx
}
