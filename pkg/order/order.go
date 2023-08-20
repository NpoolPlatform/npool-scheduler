package order

import (
	"context"

	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/sentinel"
)

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem("order"); !b {
		return
	}
	sentinel.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem("order"); !b {
		return
	}
	sentinel.Finalize()
}
