package obselete

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/transfer/bookkeeping"
	"github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/transfer/unlockaccount"
	"github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/unlockbalance"
	"github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/wait"
)

const subsystem = "paymentobselete"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	wait.Initialize(ctx, cancel, &running)
	unlockbalance.Initialize(ctx, cancel, &running)
	bookkeeping.Initialize(ctx, cancel, &running)
	unlockaccount.Initialize(ctx, cancel, &running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	unlockaccount.Finalize(ctx)
	bookkeeping.Finalize(ctx)
	unlockbalance.Finalize(ctx)
	wait.Finalize(ctx)
}
