package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
)

type Executor interface {
	Feed(*coinmwpb.Coin)
	Finalize()
}

type exec struct {
	persistent chan *types.PersistentCoin
	notif      chan *types.PersistentCoin
	feeder     chan *coinmwpb.Coin
	w          *watcher.Watcher
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent, notif chan *types.PersistentCoin) Executor {
	e := &exec{
		feeder:     make(chan *coinmwpb.Coin),
		persistent: persistent,
		notif:      notif,
		w:          watcher.NewWatcher(),
	}

	go action.Watch(ctx, cancel, e.run)
	return e
}

func (e *exec) execCoin(ctx context.Context, coin *coinmwpb.Coin) error {
	h := &coinHandler{
		Coin:       coin,
		persistent: e.persistent,
		notif:      e.notif,
	}
	return h.exec(ctx)
}

func (e *exec) handler(ctx context.Context) bool {
	select {
	case coin := <-e.feeder:
		if err := e.execCoin(ctx, coin); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "execCoin",
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
		close(e.w.ClosedChan())
		return true
	case <-e.w.CloseChan():
		close(e.w.ClosedChan())
		return true
	}
}

func (e *exec) run(ctx context.Context) {
	for {
		if b := e.handler(ctx); b {
			break
		}
	}
}

func (e *exec) Finalize() {
	if e != nil && e.w != nil {
		e.w.Shutdown()
		close(e.feeder)
	}
}

func (e *exec) Feed(coin *coinmwpb.Coin) {
	e.feeder <- coin
}
