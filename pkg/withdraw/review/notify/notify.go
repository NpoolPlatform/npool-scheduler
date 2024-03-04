package benefit

import (
	"context"
	"math"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/notif"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/sentinel"
)

const subsystem = "withdrawreviewnotify"

var (
	h *base.Handler
)

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(1*time.Hour),
		base.WithScanner(sentinel.NewSentinel()),
		base.WithExec(executor.NewExecutor()),
		base.WithRunningConcurrent(math.MaxInt),
		base.WithNotify(notif.NewNotif()),
		base.WithPersistenter(persistent.NewPersistent()),
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

func Finalize(ctx context.Context) {
	if h != nil {
		h.Finalize(ctx)
	}
}
