package gasfeeder

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/sentinel"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
)

var locked = false

const subsystem = "gasfeeder"

type handler struct {
	exec       chan *coinmwpb.Coin
	persistent chan *types.PersistentCoin
	notif      chan *types.PersistentCoin
	w          *watcher.Watcher
	executor   executor.Executor
}

func lockKey() string {
	return fmt.Sprintf("%v:%v", basetypes.Prefix_PrefixScheduler, subsystem)
}

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	if err := redis2.TryLock(lockKey(), 0); err != nil {
		logger.Sugar().Infow(
			"Initialize",
			"Error", err,
		)
		return
	}

	locked = true

	h := &handler{
		exec:       make(chan *coinmwpb.Coin),
		persistent: make(chan *types.PersistentCoin),
		w:          watcher.NewWatcher(),
	}
	sentinel.Initialize(ctx, cancel, h.exec)
	h.executor = executor.NewExecutor(ctx, cancel, h.persistent, h.notif)
	go action.Watch(ctx, cancel, h.run)
}

func (h *handler) execCoin(ctx context.Context, coin *coinmwpb.Coin) error {
	h.executor.Feed(coin)
	return nil
}

func (h *handler) persistentCoin(ctx context.Context, coin *types.PersistentCoin) error {
	logger.Sugar().Infow(
		"persistentCoin",
		"Coin", coin,
		"Error", coin.Error,
	)
	return nil
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case coin := <-h.exec:
		if err := h.execCoin(ctx, coin); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "execCoin",
				"Error", err,
			)
		}
		return false
	case coin := <-h.persistent:
		if err := h.persistentCoin(ctx, coin); err != nil {
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
		close(h.w.ClosedChan())
		return true
	case <-h.w.CloseChan():
		close(h.w.ClosedChan())
		return true
	}
}

func (h *handler) run(ctx context.Context) {
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	sentinel.Finalize()
	if locked {
		_ = redis2.Unlock(lockKey())
	}
}
