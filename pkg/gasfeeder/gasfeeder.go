package gasfeeder

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/sentinel"
)

const subsystem = "gasfeeder"

var h *base.Handler

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(time.Minute),
		base.WithScanner(sentinel.NewSentinel()),
		base.WithExec(executor.NewExecutor()),
		base.WithPersistenter(persistent.NewPersistent()),
	)
	if err != nil {
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