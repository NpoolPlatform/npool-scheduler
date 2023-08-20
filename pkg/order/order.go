package order

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment"
)

var locked = false

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem("order"); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", "order",
	)

	key := basetypes.Prefix_PrefixSchedulerOrder.String()
	if err := redis2.TryLock(key, 0); err != nil {
		logger.Sugar().Infow(
			"Initialize",
			"Error", err,
		)
		return
	}

	locked = true
	payment.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem("order"); !b {
		return
	}
	payment.Finalize()
	key := basetypes.Prefix_PrefixSchedulerOrder.String()
	if locked {
		_ = redis2.Unlock(key)
	}
}
