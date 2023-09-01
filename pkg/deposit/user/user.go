package user

import (
	"context"
	"sync"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user/notif"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user/sentinel"
)

const subsystem = "deposituser"

var h *base.Handler

func Initialize(ctx context.Context, cancel context.CancelFunc, running *sync.Map) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(30*time.Second),
		base.WithScanner(sentinel.NewSentinel()),
		base.WithNotify(notif.NewNotif()),
		base.WithExec(executor.NewExecutor()),
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

func Finalize() {
	if h != nil {
		h.Finalize()
	}
}
