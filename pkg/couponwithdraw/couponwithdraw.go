package couponwithdraw

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/couponwithdraw/approved"
	"github.com/NpoolPlatform/npool-scheduler/pkg/couponwithdraw/reviewing"
)

const subsystem = "couponwithdraw"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	approved.Initialize(ctx, cancel, &running)
	reviewing.Initialize(ctx, cancel, &running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	reviewing.Finalize(ctx)
	approved.Finalize(ctx)
}
