package order

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/precancel"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/preexpired"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/paid"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/achievement"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/bookkept"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/check"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/finish"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/received"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/spent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/stock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/timeout"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/transfer"
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

	achievement.Initialize(ctx, cancel)
	bookkept.Initialize(ctx, cancel)
	check.Initialize(ctx, cancel)
	commission.Initialize(ctx, cancel)
	finish.Initialize(ctx, cancel)
	received.Initialize(ctx, cancel)
	spent.Initialize(ctx, cancel)
	stock.Initialize(ctx, cancel)
	timeout.Initialize(ctx, cancel)
	transfer.Initialize(ctx, cancel)
	paid.Initialize(ctx, cancel)
	finish.Initialize(ctx, cancel)
	expiry.Initialize(ctx, cancel)
	precancel.Initialize(ctx, cancel)
	preexpired.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	preexpired.Finalize()
	precancel.Finalize()
	expiry.Finalize()
	finish.Finalize()
	paid.Finalize()
	transfer.Finalize()
	timeout.Finalize()
	stock.Finalize()
	spent.Finalize()
	received.Finalize()
	finish.Finalize()
	commission.Finalize()
	check.Finalize()
	bookkept.Finalize()
	achievement.Finalize()
}
