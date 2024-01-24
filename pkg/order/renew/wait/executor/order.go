package executor

import (
	"context"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/wait/types"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent           chan interface{}
	done                 chan interface{}
	notif                chan interface{}
	notifyElectricityFee bool
	notifyTechniqueFee   bool
	notifiable           bool
}

func (h *orderHandler) checkNotifiable() bool {
	now := uint32(time.Now().Unix())
	if h.StartAt >= now || h.EndAt <= now {
		return false
	}
	if h.MainAppGood.PackageWithRequireds {
		return false
	}

	orderElapsed := now - h.StartAt
	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour

	ignoredSeconds := outOfGas + compensate
	feeElapsed := orderElapsed - ignoredSeconds

	if h.ElectricityFeeAppGood != nil {
		h.notifyElectricityFee = h.ElectricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}
	if h.TechniqueFeeAppGood != nil && h.TechniqueFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByCash {
		h.notifyTechniqueFee = h.TechniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}

	h.notifiable = h.notifyElectricityFee || h.notifyTechniqueFee
	return h.notifiable
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"notifiable", h.notifiable,
			"notifyTechniqueFee", h.notifyTechniqueFee,
			"notifyElectricityFee", h.notifyElectricityFee,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, h.Order, h.notif)
	}
	if h.notifiable {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	var yes bool
	defer h.final(ctx, &err)

	if err = h.GetRequireds(ctx); err != nil {
		return err
	}
	if err := h.GetAppGoods(ctx); err != nil {
		return err
	}
	if yes, err = h.RenewGoodExist(); err != nil || !yes {
		return err
	}
	if err = h.GetRenewableOrders(ctx); err != nil {
		return err
	}
	if yes = h.checkNotifiable(); !yes {
		return nil
	}
	return nil
}
