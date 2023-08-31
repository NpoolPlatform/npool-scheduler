package order

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/finish"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/transfer"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/precancel"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/preexpired"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/start"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/timeout"
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
	finish.Initialize(ctx, cancel)
	expiry.Initialize(ctx, cancel)
	timeout.Initialize(ctx, cancel)
	precancel.Initialize(ctx, cancel)
	preexpired.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	preexpired.Finalize()
	precancel.Finalize()
	timeout.Finalize()
	expiry.Finalize()
	finish.Finalize()
	start.Finalize()
	transfer.Finalize()
	payment.Finalize()
}
