package withdraw

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/created"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/transferring"
)

const subsystem = "withdraw"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	transferring.Initialize(ctx, cancel, &running)
	created.Initialize(ctx, cancel, &running)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	created.Finalize()
	transferring.Finalize()
}
