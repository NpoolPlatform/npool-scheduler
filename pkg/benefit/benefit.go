package benefit

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/done"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/fail"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/transferring"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

const subsystem = "benefit"

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
	fail.Initialize(ctx, cancel, &running)
	done.Initialize(ctx, cancel, &running)
	transferring.Initialize(ctx, cancel, &running)
	bookkeeping.Initialize(ctx, cancel, &running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	bookkeeping.Finalize(ctx)
	transferring.Finalize(ctx)
	done.Finalize(ctx)
	fail.Finalize(ctx)
	wait.Finalize(ctx)
}
