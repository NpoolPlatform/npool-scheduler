package persistent

import (
	"context"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
)

type Persistent interface {
	Feed(*types.PersistentCoin)
	Finalize()
}

type persistent struct {
	feeder chan *types.PersistentCoin
	w      *watcher.Watcher
}

func NewPersistent(ctx context.Context, cancel context.CancelFunc) Persistent {
	p := &persistent{
		feeder: make(chan *types.PersistentCoin),
		w:      watcher.NewWatcher(),
	}

	go action.Watch(ctx, cancel, p.run)
	return p
}

func (p *persistent) persistentCoin(ctx context.Context, coin *types.PersistentCoin) error {
	txType := basetypes.TxType_TxFeedGas
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		CoinTypeID:    &coin.FeeCoinTypeID,
		FromAccountID: &coin.FromAccountID,
		ToAccountID:   &coin.ToAccountID,
		Amount:        &coin.Amount,
		FeeAmount:     &coin.FeeAmount,
		Extra:         &coin.Extra,
		Type:          &txType,
	}); err != nil {
		return err
	}
	return nil
}

func (p *persistent) handler(ctx context.Context) bool {
	select {
	case coin := <-p.feeder:
		if err := p.persistentCoin(ctx, coin); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "persistentCoin",
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

func (p *persistent) Feed(coin *types.PersistentCoin) {
	p.feeder <- coin
}
