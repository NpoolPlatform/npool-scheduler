package benefit

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	powerrental "github.com/NpoolPlatform/npool-scheduler/pkg/notif/benefit/powerrental"
)

const subsystem = "notifbenefit"

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)
	powerrental.Initialize(ctx, cancel)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	powerrental.Finalize(ctx)
}
