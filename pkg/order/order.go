package order

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	fee "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee"
	powerrental "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental"
	powerrentalsimulate "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate"
)

const subsystem = "order"

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)
	powerrental.Initialize(ctx, cancel)
	fee.Initialize(ctx, cancel)
	powerrentalsimulate.Initialize(ctx, cancel)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	powerrentalsimulate.Initialize(ctx, cancel)
	fee.Initialize(ctx, cancel)
	powerrental.Initialize(ctx, cancel)
}
