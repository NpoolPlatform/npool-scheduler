package powerrental

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	cancelachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/achievement"
	cancelbookkeeping "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/bookkeeping"
	cancelcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/check"
	cancelcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/precancel"
	cancelrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/restorestock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/returnbalance"
	cancelunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/unlockaccount"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/created"
	expirycheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/expiry/check"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/expiry/preexpired"
	expiryrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/expiry/restorestock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/createorderuser"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/deleteproportion"
	miningpoolpreexpired "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/preexpired"
	miningpoolrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/restorestock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/setproportion"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/setrevenueaddress"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/waitstart"
	paidcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/check"
	paidstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock"
	paymentachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/achievement"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/bookkeeping"
	paymentcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/received"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/spend"
	paymentstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/stock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/timeout"
	paymentunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/unlockaccount"
	paymentwait "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/wait"
	renewcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/check"
	renewexecute "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/execute"
	renewnotify "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/notify"
	renewwait "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/wait"
)

const subsystem = "orderpowerrental"

var running sync.Map

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", subsystem,
	)

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
	paidcheck.Initialize(ctx, cancel, &running)
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
	created.Initialize(ctx, cancel, &running)
	renewwait.Initialize(ctx, cancel, &running)
	renewcheck.Initialize(ctx, cancel, &running)
	renewnotify.Initialize(ctx, cancel, &running)
	renewexecute.Initialize(ctx, cancel, &running)

	// for miningpool
	waitstart.Initialize(ctx, cancel, &running)
	createorderuser.Initialize(ctx, cancel, &running)
	setproportion.Initialize(ctx, cancel, &running)
	setrevenueaddress.Initialize(ctx, cancel, &running)
	miningpoolpreexpired.Initialize(ctx, cancel, &running)
	deleteproportion.Initialize(ctx, cancel, &running)
	miningpoolrestorestock.Initialize(ctx, cancel, &running)
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
	paidcheck.Finalize(ctx)
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

	// for miningpool
	waitstart.Finalize(ctx)
	createorderuser.Finalize(ctx)
	setproportion.Finalize(ctx)
	setrevenueaddress.Finalize(ctx)
	miningpoolpreexpired.Finalize(ctx)
	deleteproportion.Finalize(ctx)
	miningpoolrestorestock.Finalize(ctx)
}
