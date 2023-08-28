package benefit

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/done"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/fail"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/transferring"
	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

const subsystem = "benefit"

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	wait.Initialize(ctx, cancel)
	fail.Initialize(ctx, cancel)
	done.Initialize(ctx, cancel)
	transferring.Initialize(ctx, cancel)
	bookkeeping.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	bookkeeping.Finalize()
	transferring.Finalize()
	done.Finalize()
	fail.Finalize()
	wait.Finalize()
}
