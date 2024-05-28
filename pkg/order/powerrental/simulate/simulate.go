package powerrental

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	cancelachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/achievement"
	cancelbookkeeping "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/bookkeeping"
	cancelcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/check"
	cancelchildcanceledparent "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/childcanceledparent"
	cancelcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/precancel"
	cancelrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/restorestock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/returnbalance"
	cancelunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/unlockaccount"
	cancelupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/updatechilds"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/created"
	expirycheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/check"
	expirychildexpiredparent "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/childexpiredparent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/preexpired"
	expiryrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/restorestock"
	expiryupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/updatechilds"
	paidcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/paid/check"
	paidchildinserviceparent "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/paid/childinserviceparent"
	paidstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/paid/stock"
	paidupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/paid/updatechilds"
	paymentachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/achievement"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/bookkeeping"
	paymentchildpaidparent "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/childpaidparent"
	paymentcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/finish"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/received"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/spend"
	paymentstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/stock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/timeout"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/transfer"
	paymentunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/unlockaccount"
	paymentupdatechilds "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/updatechilds"
	paymentwait "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/wait"
)

const subsystem = "orderpowerrentalsimulate"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

	paidchildinserviceparent.Initialize(ctx, cancel, &running)
	paidupdatechilds.Initialize(ctx, cancel, &running)
	paymentupdatechilds.Initialize(ctx, cancel, &running)
	paymentunlockaccount.Initialize(ctx, cancel, &running)
	paymentachievement.Initialize(ctx, cancel, &running)
	bookkeeping.Initialize(ctx, cancel, &running)
	paymentwait.Initialize(ctx, cancel, &running)
	paymentchildpaidparent.Initialize(ctx, cancel, &running)
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
	cancelcheck.Initialize(ctx, cancel, &running)
	cancelachievement.Initialize(ctx, cancel, &running)
	cancelrestorestock.Initialize(ctx, cancel, &running)
	returnbalance.Initialize(ctx, cancel, &running)
	preexpired.Initialize(ctx, cancel, &running)
	expiryrestorestock.Initialize(ctx, cancel, &running)
	expirycheck.Initialize(ctx, cancel, &running)
	cancelchildcanceledparent.Initialize(ctx, cancel, &running)
	cancelupdatechilds.Initialize(ctx, cancel, &running)
	expirychildexpiredparent.Initialize(ctx, cancel, &running)
	expiryupdatechilds.Initialize(ctx, cancel, &running)
	created.Initialize(ctx, cancel, &running)
	renewwait.Initialize(ctx, cancel, &running)
	renewcheck.Initialize(ctx, cancel, &running)
	renewnotify.Initialize(ctx, cancel, &running)
	renewexecute.Initialize(ctx, cancel, &running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	renewexecute.Finalize(ctx)
	renewnotify.Finalize(ctx)
	renewcheck.Finalize(ctx)
	renewwait.Finalize(ctx)
	created.Finalize(ctx)
	expiryupdatechilds.Finalize(ctx)
	expirychildexpiredparent.Finalize(ctx)
	cancelupdatechilds.Finalize(ctx)
	cancelchildcanceledparent.Finalize(ctx)
	expirycheck.Finalize(ctx)
	expiryrestorestock.Finalize(ctx)
	preexpired.Finalize(ctx)
	returnbalance.Finalize(ctx)
	cancelrestorestock.Finalize(ctx)
	cancelachievement.Finalize(ctx)
	cancelcheck.Finalize(ctx)
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
	paymentchildpaidparent.Finalize(ctx)
	paymentwait.Finalize(ctx)
	bookkeeping.Finalize(ctx)
	paymentachievement.Finalize(ctx)
	paymentunlockaccount.Finalize(ctx)
	paymentupdatechilds.Finalize(ctx)
	paidupdatechilds.Finalize(ctx)
	paidchildinserviceparent.Finalize(ctx)
}
