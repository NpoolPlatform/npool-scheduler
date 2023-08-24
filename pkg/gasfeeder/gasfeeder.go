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
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/sentinel"
)

var locked = false

const subsystem = "gasfeeder"

type handler struct {
	exec chan *coinmwpb.Coin
	w    *watcher.Watcher
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
		exec: make(chan *coinmwpb.Coin),
		w:    watcher.NewWatcher(),
	}
	sentinel.Initialize(ctx, cancel, h.exec)
	go action.Watch(ctx, cancel, h.run)
}

func (h *handler) handler(ctx context.Context) bool {
	select {
	case <-h.exec:
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
