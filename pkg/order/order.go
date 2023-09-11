package order

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	cancelachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/achievement"
	cancelbookkeeping "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/bookkeeping"
	cancelcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/precancel"
	cancelrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/restorestock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/returnbalance"
	cancelunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/unlockaccount"
	cancelupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/updatechilds"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/created"
	expirycheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/check"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/preexpired"
	expiryrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/restorestock"
	expiryupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/updatechilds"
	paidcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/paid/check"
	paidstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/paid/stock"
	paidupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/paid/updatechilds"
	paymentachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/achievement"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/bookkeeping"
	paymentcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/finish"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/received"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/spend"
	paymentstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/stock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/timeout"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/transfer"
	paymentunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/unlockaccount"
	paymentupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/updatechilds"
	paymentwait "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/wait"
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

	paidupdatechilds.Initialize(ctx, cancel, &running)
	paymentupdatechilds.Initialize(ctx, cancel, &running)
	paymentunlockaccount.Initialize(ctx, cancel, &running)
	paymentachievement.Initialize(ctx, cancel, &running)
	bookkeeping.Initialize(ctx, cancel, &running)
	paymentwait.Initialize(ctx, cancel, &running)
	paymentcommission.Initialize(ctx, cancel, &running)
	cancelcommission.Initialize(ctx, cancel, &running)
	received.Initialize(ctx, cancel, &running)
	spend.Initialize(ctx, cancel, &running)
	paidstock.Initialize(ctx, cancel, &running)
	paymentstock.Initialize(ctx, cancel, &running)
	timeout.Initialize(ctx, cancel, &running)
	transfer.Initialize(ctx, cancel, &running)
	paidcheck.Initialize(ctx, cancel, &running)
	finish.Initialize(ctx, cancel, &running)
	precancel.Initialize(ctx, cancel, &running)
	cancelbookkeeping.Initialize(ctx, cancel, &running)
	cancelunlockaccount.Initialize(ctx, cancel, &running)
	cancelachievement.Initialize(ctx, cancel, &running)
	cancelrestorestock.Initialize(ctx, cancel, &running)
	returnbalance.Initialize(ctx, cancel, &running)
	preexpired.Initialize(ctx, cancel, &running)
	expiryrestorestock.Initialize(ctx, cancel, &running)
	expirycheck.Initialize(ctx, cancel, &running)
	cancelupdatechilds.Initialize(ctx, cancel, &running)
	expiryupdatechilds.Initialize(ctx, cancel, &running)
	created.Initialize(ctx, cancel, &running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	created.Finalize(ctx)
	expiryupdatechilds.Finalize(ctx)
	cancelupdatechilds.Finalize(ctx)
	expirycheck.Finalize(ctx)
	expiryrestorestock.Finalize(ctx)
	preexpired.Finalize(ctx)
	returnbalance.Finalize(ctx)
	cancelrestorestock.Finalize(ctx)
	cancelachievement.Finalize(ctx)
	cancelunlockaccount.Finalize(ctx)
	cancelbookkeeping.Finalize(ctx)
	precancel.Finalize(ctx)
	finish.Finalize(ctx)
	paidcheck.Finalize(ctx)
	transfer.Finalize(ctx)
	timeout.Finalize(ctx)
	paymentstock.Finalize(ctx)
	paidstock.Finalize(ctx)
	spend.Finalize(ctx)
	received.Finalize(ctx)
	cancelcommission.Finalize(ctx)
	paymentcommission.Finalize(ctx)
	paymentwait.Finalize(ctx)
	bookkeeping.Finalize(ctx)
	paymentachievement.Finalize(ctx)
	paymentunlockaccount.Finalize(ctx)
	paymentupdatechilds.Finalize(ctx)
	paidupdatechilds.Finalize(ctx)
}
