package order

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/transfer"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/start"
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

	payment.Initialize(ctx, cancel)
	transfer.Initialize(ctx, cancel)
	start.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	start.Finalize()
	transfer.Finalize()
	payment.Finalize()
}
