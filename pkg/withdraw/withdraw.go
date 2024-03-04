package withdraw

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/approved"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/created"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/fail/prefail"
	failreturnbalance "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/fail/returnbalance"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/rejected/prerejected"
	rejectedreturnbalance "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/rejected/returnbalance"
	withdrawreviewnotify "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/reviewing"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/presuccessful"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/spendbalance"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/transferring"
)

const subsystem = "withdraw"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	transferring.Initialize(ctx, cancel, &running)
	created.Initialize(ctx, cancel, &running)
	approved.Initialize(ctx, cancel, &running)
	reviewing.Initialize(ctx, cancel, &running)
	prefail.Initialize(ctx, cancel, &running)
	failreturnbalance.Initialize(ctx, cancel, &running)
	rejectedreturnbalance.Initialize(ctx, cancel, &running)
	prerejected.Initialize(ctx, cancel, &running)
	presuccessful.Initialize(ctx, cancel, &running)
	spendbalance.Initialize(ctx, cancel, &running)
	withdrawreviewnotify.Initialize(ctx, cancel)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	withdrawreviewnotify.Finalize(ctx)
	spendbalance.Finalize(ctx)
	presuccessful.Finalize(ctx)
	prerejected.Finalize(ctx)
	rejectedreturnbalance.Finalize(ctx)
	failreturnbalance.Finalize(ctx)
	prefail.Finalize(ctx)
	reviewing.Finalize(ctx)
	approved.Finalize(ctx)
	created.Finalize(ctx)
	transferring.Finalize(ctx)
}
