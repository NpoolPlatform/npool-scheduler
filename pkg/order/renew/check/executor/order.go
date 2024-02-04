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
	persistent        chan interface{}
	done              chan interface{}
	notif             chan interface{}
	newRenewState     ordertypes.OrderRenewState
	notifiable        bool
	nextRenewNotifyAt uint32
}

//nolint:gocognit
func (h *orderHandler) checkNotifiable() bool {
	now := uint32(time.Now().Unix())
	if h.StartAt >= now || h.EndAt <= now {
		return false
	}

	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour
	ignoredSeconds := outOfGas + compensate
	nextNotifyAt := now

	const minNotifyInterval = timedef.SecondsPerHour
	const preNotifyTicker = timedef.SecondsPerHour * 24
	const noNotifyTicker = minNotifyInterval

	if h.ExistUnpaidElectricityFeeOrder || h.ExistUnpaidTechniqueFeeOrder {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = now + noNotifyTicker
		return false
	}

	if h.ElectricityFeeAppGood != nil {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		if h.ElectricityFeeEndAt < h.EndAt {
			if h.CheckElectricityFee {
				nextNotifyAt = now + minNotifyInterval
				h.newRenewState = ordertypes.OrderRenewState_OrderRenewNotify
			} else {
				nextNotifyAt = h.ElectricityFeeEndAt - preNotifyTicker
			}
		} else {
			nextNotifyAt = h.EndAt + noNotifyTicker
		}
	}
	if h.TechniqueFeeAppGood != nil && h.TechniqueFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByCash {
		h.TechniqueFeeEndAt = h.StartAt + h.TechniqueFeeDuration + ignoredSeconds
		if h.newRenewState == h.RenewState {
			h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		}
		if h.TechniqueFeeEndAt < h.EndAt {
			if h.CheckTechniqueFee {
				if nextNotifyAt == now {
					nextNotifyAt = now + minNotifyInterval
				} else {
					nextNotifyAt = uint32(math.Min(float64(nextNotifyAt), float64(now+minNotifyInterval)))
				}
				h.newRenewState = ordertypes.OrderRenewState_OrderRenewNotify
			} else {
				if nextNotifyAt == now {
					nextNotifyAt = h.TechniqueFeeEndAt - preNotifyTicker
				} else {
					nextNotifyAt = uint32(math.Min(float64(nextNotifyAt), float64(h.TechniqueFeeEndAt-preNotifyTicker)))
				}
			}
		} else {
			if nextNotifyAt == now {
				nextNotifyAt = h.EndAt + noNotifyTicker
			} else {
				nextNotifyAt = uint32(math.Min(float64(nextNotifyAt), float64(h.EndAt+noNotifyTicker)))
			}
		}
	}

	h.notifiable = h.CheckElectricityFee || h.CheckTechniqueFee
	h.nextRenewNotifyAt = nextNotifyAt

	if h.ElectricityFeeAppGood == nil && h.TechniqueFeeAppGood == nil {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = h.EndAt + noNotifyTicker
	}
	if (h.ElectricityFeeAppGood != nil && h.ElectricityFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByProfit) &&
		(h.TechniqueFeeAppGood != nil && h.TechniqueFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByProfit) {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = h.EndAt + noNotifyTicker
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
			"CheckElectricityFee", h.CheckElectricityFee,
			"CheckTechniqueFee", h.CheckTechniqueFee,
			"nextRenewNotifyAt", h.nextRenewNotifyAt,
			"ExistUnpaidTechniqueFee", h.ExistUnpaidTechniqueFeeOrder,
			"ExistUnpaidElectricityFee", h.ExistUnpaidElectricityFeeOrder,
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
