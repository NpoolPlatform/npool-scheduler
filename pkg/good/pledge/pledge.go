package pledge

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/pledge/checkdeployment"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/pledge/deployment"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/pledge/wait"
)

const subsystem = "goodpledge"

func Initialize(ctx context.Context, cancel context.CancelFunc, running *sync.Map) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)
	wait.Initialize(ctx, cancel, running)
	deployment.Initialize(ctx, cancel, running)
	checkdeployment.Initialize(ctx, cancel, running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	wait.Finalize(ctx)
	deployment.Finalize(ctx)
	checkdeployment.Finalize(ctx)
}
