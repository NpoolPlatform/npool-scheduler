package announcement

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/sentinel"
)

const subsystem = "announcement"

var h *base.Handler

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(time.Minute),
		base.WithScanner(sentinel.NewSentinel()),
		base.WithExec(executor.NewExecutor()),
		base.WithExecutorNumber(4),
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

func Finalize() {
	if h != nil {
		h.Finalize()
	}
}
