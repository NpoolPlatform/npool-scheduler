package powerrental

import (
	"context"
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	cancelachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/achievement"
	cancelbookkeeping "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/bookkeeping"
	cancelcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/check"
	cancelcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/precancel"
	cancelrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/restorestock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/returnbalance"
	cancelunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/unlockaccount"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/created"
	expirycheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/check"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/preexpired"
	expiryrestorestock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/expiry/restorestock"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/miningpool/checkpoolbalance"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/miningpool/checkproportion"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/miningpool/createorderuser"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/miningpool/deleteproportion"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/miningpool/setproportion"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/miningpool/setrevenueaddress"
	paidcheck "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/paid/check"
	paidstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/paid/stock"
	paymentachievement "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/achievement"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/bookkeeping"
	paymentcommission "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/commission"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/received"
	"github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/spend"
	paymentstock "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/stock"
	paymentunlockaccount "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/payment/unlockaccount"
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

	// for miningpool
	createorderuser.Initialize(ctx, cancel, &running)
	checkproportion.Initialize(ctx, cancel, &running)
	setproportion.Initialize(ctx, cancel, &running)
	setrevenueaddress.Initialize(ctx, cancel, &running)
	deleteproportion.Initialize(ctx, cancel, &running)
	checkpoolbalance.Initialize(ctx, cancel, &running)
}

func Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(subsystem); !b {
		return
	}
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
	createorderuser.Finalize(ctx)
	checkproportion.Finalize(ctx)
	setproportion.Finalize(ctx)
	setrevenueaddress.Finalize(ctx)
	deleteproportion.Finalize(ctx)
	checkpoolbalance.Finalize(ctx)
}
