package stock

import (
	"context"
	"sync"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock/reward"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock/sentinel"
)

const subsystem = "orderpowerrentalpaidstock"

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
		base.WithRewarder(reward.NewReward()),
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
