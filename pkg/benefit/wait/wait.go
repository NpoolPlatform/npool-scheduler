package wait

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/notif"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/sentinel"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/types"
)

const subsystem = "benefitwait"

var h *base.Handler

func Initialize(ctx context.Context, cancel context.CancelFunc, running *sync.Map) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(1*time.Minute),
		base.WithScanner(sentinel.NewSentinel()),
		base.WithExec(executor.NewExecutor()),
		base.WithRunningConcurrent(math.MaxInt),
		base.WithNotify(notif.NewNotif()),
		base.WithPersistenter(persistent.NewPersistent()),
		base.WithRunningMap(running),
	)
	if err != nil || _h == nil {
		logger.Sugar().Errorw(
			"Initialize",
			"Subsystem", subsystem,
			"Error", err,
		)
		return
	}

	h = _h
	go h.Run(ctx, cancel)
}

func Trigger(cond *types.TriggerCond) {
	if h != nil {
		h.Trigger(cond)
	}
}

func Finalize(ctx context.Context) {
	if h != nil {
		h.Finalize(ctx)
	}
}
