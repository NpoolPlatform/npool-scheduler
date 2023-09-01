package deposit

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/finish"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/transfer"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user"
)

const subsystem = "deposit"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	user.Initialize(ctx, cancel, &running)
	finish.Initialize(ctx, cancel, &running)
	transfer.Initialize(ctx, cancel, &running)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	transfer.Finalize()
	finish.Finalize()
	user.Finalize()
}
