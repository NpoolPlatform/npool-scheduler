package scheduler

import (
	"context"

	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit"
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder"
	"github.com/NpoolPlatform/npool-scheduler/pkg/limitation"
	"github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement"
	notifbenefit "github.com/NpoolPlatform/npool-scheduler/pkg/notif/benefit"
	"github.com/NpoolPlatform/npool-scheduler/pkg/notif/notification"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw"
)

func Finalize(ctx context.Context) {
	notifbenefit.Finalize(ctx)
	benefit.Finalize(ctx)
	deposit.Finalize(ctx)
	withdraw.Finalize(ctx)
	limitation.Finalize(ctx)
	txqueue.Finalize(ctx)
	announcement.Finalize(ctx)
	notification.Finalize(ctx)
	gasfeeder.Finalize(ctx)
	order.Finalize(ctx)
}

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	order.Initialize(ctx, cancel)
	gasfeeder.Initialize(ctx, cancel)
	announcement.Initialize(ctx, cancel)
	notification.Initialize(ctx, cancel)
	txqueue.Initialize(ctx, cancel)
	limitation.Initialize(ctx, cancel)
	withdraw.Initialize(ctx, cancel)
	deposit.Initialize(ctx, cancel)
	benefit.Initialize(ctx, cancel)
	notifbenefit.Initialize(ctx, cancel)
}

type initializer struct {
	init  func(context.Context, context.CancelFunc)
	final func(context.Context)
}

var subsystems = map[string]initializer{
	"order":        {order.Initialize, order.Finalize},
	"gasfeeder":    {gasfeeder.Initialize, gasfeeder.Finalize},
	"announcement": {announcement.Initialize, announcement.Finalize},
	"notification": {notification.Initialize, notification.Finalize},
	"txqueue":      {txqueue.Initialize, txqueue.Finalize},
	"limitation":   {limitation.Initialize, limitation.Finalize},
	"withdraw":     {withdraw.Initialize, withdraw.Finalize},
	"deposit":      {deposit.Initialize, deposit.Finalize},
	"benefit":      {benefit.Initialize, benefit.Finalize},
	"notifbenefit": {notifbenefit.Initialize, notifbenefit.Finalize},
}

func FinalizeSubsystem(ctx context.Context, system string) {
	_finalizer, ok := subsystems[system]
	if !ok {
		return
	}
	_finalizer.final(ctx)
}

func InitializeSubsystem(ctx context.Context, system string) {
	_initializer, ok := subsystems[system]
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	_initializer.init(ctx, cancel)
}
