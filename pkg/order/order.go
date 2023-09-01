package order

import (
	"context"
	"sync"

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

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	achievement.Initialize(ctx, cancel, &running)
	bookkept.Initialize(ctx, cancel, &running)
	check.Initialize(ctx, cancel, &running)
	commission.Initialize(ctx, cancel, &running)
	finish.Initialize(ctx, cancel, &running)
	received.Initialize(ctx, cancel, &running)
	spent.Initialize(ctx, cancel, &running)
	stock.Initialize(ctx, cancel, &running)
	timeout.Initialize(ctx, cancel, &running)
	transfer.Initialize(ctx, cancel, &running)
	paid.Initialize(ctx, cancel, &running)
	finish.Initialize(ctx, cancel, &running)
	expiry.Initialize(ctx, cancel, &running)
	precancel.Initialize(ctx, cancel, &running)
	preexpired.Initialize(ctx, cancel, &running)
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
