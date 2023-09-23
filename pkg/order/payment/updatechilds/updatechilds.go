package updatechilds

import (
	"context"
	"sync"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/updatechilds/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/updatechilds/notif"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/updatechilds/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/updatechilds/sentinel"
)

const subsystem = "orderpaymentupdatechilds"

var h *base.Handler

func Initialize(ctx context.Context, cancel context.CancelFunc, running *sync.Map) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(time.Second),
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
