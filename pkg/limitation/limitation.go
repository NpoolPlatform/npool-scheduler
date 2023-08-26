package limitation

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/limitation/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/limitation/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/limitation/sentinel"
)

var locked = false

const subsystem = "limitation"

type handler struct {
	persistent   chan interface{}
	notif        chan interface{}
	w            *watcher.Watcher
	executor     baseexecutor.Executor
	persistenter basepersistent.Persistent
}

var h *handler

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

	h = &handler{
		persistent: make(chan interface{}),
		notif:      make(chan interface{}),
		w:          watcher.NewWatcher(),
	}
	sentinel.Initialize(ctx, cancel)
	h.executor = executor.NewExecutor(ctx, cancel, h.persistent, h.notif)
	h.persistenter = persistent.NewPersistent(ctx, cancel)
	go action.Watch(ctx, cancel, h.run)
}

func (h *handler) execCoin(ctx context.Context, coin interface{}) error {
	h.executor.Feed(coin)
	return nil
}

func (h *handler) persistentCoin(ctx context.Context, coin interface{}) error {
	h.persistenter.Feed(coin)
	return nil
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case coin := <-sentinel.Exec():
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

func (h *handler) finalize() {
	if h.w != nil {
		h.w.Shutdown()
	}
	close(h.persistent)
	close(h.notif)
	h.executor.Finalize()
	h.persistenter.Finalize()
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
	if h != nil {
		h.finalize()
	}
	if locked {
		_ = redis2.Unlock(lockKey())
	}
}
