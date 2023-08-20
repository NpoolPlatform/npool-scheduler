package order

import (
	"context"

	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment"
)

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem("order"); !b {
		return
	}
	payment.Initialize(ctx, cancel)
}

func Finalize() {
	if b := config.SupportSubsystem("order"); !b {
		return
	}
	payment.Finalize()
}
