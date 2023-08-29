package txqueue

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/transferring"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/wait"
)

const subsystem = "txqueue"

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	created.Initialize(ctx, cancel)
	wait.Initialize(ctx, cancel)
	transferring.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	transferring.Finalize()
	wait.Finalize()
	created.Finalize()
}
