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
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/check/types"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/common"
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
func (h *orderHandler) checkNotifiable(ctx context.Context) (bool, error) {
	now := uint32(time.Now().Unix())
	const minNotifyInterval = timedef.SecondsPerHour
	const preNotifyTicker = timedef.SecondsPerHour * 24
	const noNotifyTicker = minNotifyInterval

	if h.StartAt >= now {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = h.StartAt
		return false, nil
	}
	if h.EndAt <= now {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = math.MaxUint32
		return false, nil
	}

	nextNotifyAt := now

	if ((h.ElectricityFee == nil ||
		(h.ElectricityFee != nil && h.ElectricityFee.SettlementType != goodtypes.GoodSettlementType_GoodSettledByPaymentAmount)) &&
		(h.TechniqueFee == nil ||
			(h.TechniqueFee != nil && h.TechniqueFee.SettlementType != goodtypes.GoodSettlementType_GoodSettledByPaymentAmount))) ||
		h.AppPowerRental.PackageWithRequireds {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = h.EndAt + noNotifyTicker
		return false, nil
	}

	if able, err := h.Renewable(ctx); err != nil || !able {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		h.nextRenewNotifyAt = now + noNotifyTicker
		return false, err
	}

	if h.ElectricityFee != nil {
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
	if h.TechniqueFee != nil && h.TechniqueFee.SettlementType == goodtypes.GoodSettlementType_GoodSettledByPaymentAmount {
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
	if nextNotifyAt < now {
		nextNotifyAt = now
	}

	h.notifiable = h.CheckElectricityFee || h.CheckTechniqueFee
	h.nextRenewNotifyAt = nextNotifyAt

	return h.notifiable, nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"NewRenewState", h.newRenewState,
			"notifiable", h.notifiable,
			"CheckElectricityFee", h.CheckElectricityFee,
			"CheckTechniqueFee", h.CheckTechniqueFee,
			"nextRenewNotifyAt", h.nextRenewNotifyAt,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder:  h.PowerRentalOrder,
		NewRenewState:     h.newRenewState,
		NextRenewNotifyAt: h.nextRenewNotifyAt,
	}
	if *err != nil || h.notifiable {
		asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.notif)
	}
	if h.newRenewState != h.RenewState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.newRenewState = h.RenewState

	var err error
	var yes bool
	defer h.final(ctx, &err)

	if err = h.GetAppPowerRental(ctx); err != nil {
		return err
	}
	if err = h.GetAppGoodRequireds(ctx); err != nil {
		return err
	}
	if err := h.GetAppFees(ctx); err != nil {
		return err
	}
	h.FormalizeFeeDurationSeconds()
	if err = h.CalculateRenewDuration(ctx); err != nil {
		return err
	}
	if yes, err = h.checkNotifiable(ctx); err != nil || !yes {
		return err
	}
	return nil
}
