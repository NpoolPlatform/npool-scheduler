package executor

import (
	"context"
	"math"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/check/types"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/common"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent          chan interface{}
	done                chan interface{}
	notif               chan interface{}
	newRenewState       ordertypes.OrderRenewState
	electricityFeeEndAt uint32
	techniqueFeeEndAt   uint32
	checkElectricityFee bool
	checkTechniqueFee   bool
	notifiable          bool
	nextRenewNotifyAt   uint32
}

func (h *orderHandler) checkNotifiable() bool {
	now := uint32(time.Now().Unix())
	if h.StartAt >= now || h.EndAt <= now {
		return false
	}

	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour
	ignoredSeconds := outOfGas + compensate
	nextNotifyAt := now

	if h.ElectricityFeeAppGood != nil {
		h.electricityFeeEndAt = h.StartAt + h.ElectricityFeeDuration + ignoredSeconds
		h.checkElectricityFee = h.electricityFeeEndAt <= now+timedef.SecondsPerHour*24
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		if h.electricityFeeEndAt < h.EndAt {
			if h.checkElectricityFee {
				seconds := uint32(math.Min(float64(now-h.electricityFeeEndAt), float64(timedef.SecondsPerHour*6)))
				seconds = uint32(math.Max(float64(seconds), float64(timedef.SecondsPerHour)))
				nextNotifyAt = now + seconds
				h.newRenewState = ordertypes.OrderRenewState_OrderRenewNotify
			} else {
				nextNotifyAt = h.electricityFeeEndAt - timedef.SecondsPerHour*24
			}
		} else {
			nextNotifyAt = h.EndAt + timedef.SecondsPerHour
		}
	}
	if h.TechniqueFeeAppGood != nil && h.TechniqueFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByCash {
		h.techniqueFeeEndAt = h.StartAt + h.TechniqueFeeDuration + ignoredSeconds
		h.checkTechniqueFee = h.techniqueFeeEndAt <= now+timedef.SecondsPerHour*24
		if h.newRenewState == h.RenewState {
			h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		}
		if h.techniqueFeeEndAt < h.EndAt {
			if h.checkTechniqueFee {
				seconds := uint32(math.Min(float64(now-h.techniqueFeeEndAt), float64(timedef.SecondsPerHour*6)))
				seconds = uint32(math.Max(float64(seconds), float64(timedef.SecondsPerHour)))
				if nextNotifyAt == now {
					nextNotifyAt = now + seconds
				} else {
					nextNotifyAt = uint32(math.Min(float64(nextNotifyAt), float64(now+seconds)))
				}
				h.newRenewState = ordertypes.OrderRenewState_OrderRenewNotify
			} else {
				if nextNotifyAt == now {
					nextNotifyAt = h.techniqueFeeEndAt - timedef.SecondsPerHour*24
				} else {
					nextNotifyAt = uint32(math.Min(float64(nextNotifyAt), float64(h.techniqueFeeEndAt-timedef.SecondsPerHour*24)))
				}
			}
		} else {
			if nextNotifyAt == now {
				nextNotifyAt = h.EndAt + timedef.SecondsPerHour
			} else {
				nextNotifyAt = uint32(math.Min(float64(nextNotifyAt), float64(h.EndAt+timedef.SecondsPerHour)))
			}
		}
	}

	h.notifiable = h.checkElectricityFee || h.checkTechniqueFee
	h.nextRenewNotifyAt = nextNotifyAt
	if h.ElectricityFeeAppGood == nil && h.TechniqueFeeAppGood == nil {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = h.EndAt + timedef.SecondsPerHour
	}

	return h.notifiable
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"NewRenewState", h.newRenewState,
			"notifiable", h.notifiable,
			"checkElectricityFee", h.checkElectricityFee,
			"checkTechniqueFee", h.checkTechniqueFee,
			"nextRenewNotifyAt", h.nextRenewNotifyAt,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:             h.Order,
		NewRenewState:     h.newRenewState,
		NextRenewNotifyAt: h.nextRenewNotifyAt,
	}
	if *err != nil || h.notifiable {
		asyncfeed.AsyncFeed(ctx, h.Order, h.notif)
	}
	if h.newRenewState != h.RenewState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.newRenewState = h.RenewState

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
		return err
	}
	return nil
}
