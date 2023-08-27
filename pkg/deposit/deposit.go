package deposit

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/transfer"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user"
)

const subsystem = "deposit"

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	user.Initialize(ctx, cancel)
	transfer.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	transfer.Finalize()
	user.Finalize()
}