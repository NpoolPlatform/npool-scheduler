package done

import (
	"context"
	"sync"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/done/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/done/notif"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/done/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/done/sentinel"
)

const subsystem = "benefitpowerrentaldone"

var h *base.Handler

func Initialize(ctx context.Context, cancel context.CancelFunc, running *sync.Map) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(1*time.Minute),
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

func Finalize(ctx context.Context) {
	if h != nil {
		h.Finalize(ctx)
	}
}
