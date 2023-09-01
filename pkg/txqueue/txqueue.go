package txqueue

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/transferring"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/wait"
)

const subsystem = "txqueue"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	created.Initialize(ctx, cancel, &running)
	wait.Initialize(ctx, cancel, &running)
	transferring.Initialize(ctx, cancel, &running)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	transferring.Finalize()
	wait.Finalize()
	created.Finalize()
}
