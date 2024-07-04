package unlockaccount

import (
	"context"
	"sync"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/unlockaccount/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/unlockaccount/notif"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/unlockaccount/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/unlockaccount/sentinel"
)

const subsystem = "orderpowerrentalpaymentunlockaccount"

var h *base.Handler

func Initialize(ctx context.Context, cancel context.CancelFunc, running *sync.Map) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(10*time.Second),
		base.WithScanner(sentinel.NewSentinel()),
		base.WithExec(executor.NewExecutor()),
		base.WithPersistenter(persistent.NewPersistent()),
		base.WithNotify(notif.NewNotif()),
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
