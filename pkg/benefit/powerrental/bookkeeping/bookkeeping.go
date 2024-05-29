package bookkeeping

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	goodbookkeeping "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/good"
	simulatebookkeeping "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/simulate"
	userbookkeeping "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/user"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

const subsystem = "benefitbookkeeping"

func Initialize(ctx context.Context, cancel context.CancelFunc, running *sync.Map) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	goodbookkeeping.Initialize(ctx, cancel, running)
	userbookkeeping.Initialize(ctx, cancel, running)
	simulatebookkeeping.Initialize(ctx, cancel, running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	userbookkeeping.Finalize(ctx)
	goodbookkeeping.Finalize(ctx)
	simulatebookkeeping.Finalize(ctx)
}
