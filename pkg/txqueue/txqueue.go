package txqueue

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created"
)

var locked = false

const subsystem = "txqueue"

func lockKey() string {
	return fmt.Sprintf("%v:%v", basetypes.Prefix_PrefixScheduler, subsystem)
}

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	if err := redis2.TryLock(lockKey(), 0); err != nil {
		logger.Sugar().Infow(
			"Initialize",
			"Error", err,
		)
		return
	}

	locked = true
	created.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	created.Finalize()
	if locked {
		_ = redis2.Unlock(lockKey())
	}
}
