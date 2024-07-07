package executor

import (
	"context"
	"math"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	outofgasmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/outofgas"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/check/types"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/common"
	feeordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/fee"
	outofgasmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/outofgas"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent         chan interface{}
	done               chan interface{}
	notif              chan interface{}
	newRenewState      ordertypes.OrderRenewState
	notifiable         bool
	nextRenewNotifyAt  uint32
	outOfGas           *outofgasmwpb.OutOfGas
	createOutOfGas     bool
	feeEndAt           uint32
	finishOutOfGas     bool
	outOfGasFinishedAt uint32
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

func (h *orderHandler) getOutOfGas(ctx context.Context) error {
	info, err := outofgasmwcli.GetOutOfGasOnly(ctx, &outofgasmwpb.Conds{
		OrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.OrderID},
		EndAt:   &basetypes.Uint32Val{Op: cruder.EQ, Value: 0},
	})
	if err != nil {
		return err
	}
	h.outOfGas = info
	return nil
}

func (h *orderHandler) calculateOutOfGasFinishedAt(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	finishedAt := map[goodtypes.GoodType]uint32{}

	for {
		feeOrders, _, err := feeordermwcli.GetFeeOrders(ctx, &feeordermwpb.Conds{
			ParentOrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.OrderID},
			PaidAt:        &basetypes.Uint32Val{Op: cruder.GTE, Value: h.outOfGas.StartAt},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(feeOrders) == 0 {
			break
		}
		for _, feeOrder := range feeOrders {
			_finishedAt, ok := finishedAt[feeOrder.GoodType]
			if !ok || _finishedAt == 0 || _finishedAt > feeOrder.PaidAt {
				finishedAt[feeOrder.GoodType] = feeOrder.PaidAt
			}
		}
		h.finishOutOfGas = true
		offset += limit
	}

	for _, _finishedAt := range finishedAt {
		if h.outOfGasFinishedAt == 0 || h.outOfGasFinishedAt < _finishedAt {
			h.outOfGasFinishedAt = _finishedAt
		}
	}

	return nil
}

func (h *orderHandler) resolveCreateOutOfGas() {
	now := uint32(time.Now().Unix())
	h.createOutOfGas = h.outOfGas == nil && (h.ElectricityFeeEndAt < now || h.TechniqueFeeEndAt < now) && h.InsufficientBalance
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"NewRenewState", h.newRenewState,
			"Notifiable", h.notifiable,
			"CheckElectricityFee", h.CheckElectricityFee,
			"CheckTechniqueFee", h.CheckTechniqueFee,
			"NextRenewNotifyAt", h.nextRenewNotifyAt,
			"Deductions", h.Deductions,
			"InsufficientBalance", h.InsufficientBalance,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder:   h.PowerRentalOrder,
		NewRenewState:      h.newRenewState,
		NextRenewNotifyAt:  h.nextRenewNotifyAt,
		CreateOutOfGas:     h.createOutOfGas,
		FeeEndAt:           h.feeEndAt,
		FinishOutOfGas:     h.finishOutOfGas,
		OutOfGasFinishedAt: h.outOfGasFinishedAt,
	}
	if h.outOfGas != nil {
		persistentOrder.OutOfGasEntID = h.outOfGas.EntID
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
	if err = h.CalculateUSDAmount(); err != nil {
		return err
	}
	if err := h.getOutOfGas(ctx); err != nil {
		return err
	}
	if h.outOfGas != nil {
		if err = h.calculateOutOfGasFinishedAt(ctx); err != nil {
			return err
		}
	}
	if yes, err = h.checkNotifiable(ctx); err != nil || !yes {
		return err
	}
	if err = h.GetDeductionCoins(ctx); err != nil {
		return err
	}
	if err = h.GetDeductionAppCoins(ctx); err != nil {
		return err
	}
	if err = h.GetUserLedgers(ctx); err != nil {
		return err
	}
	if err = h.GetCoinUSDCurrency(ctx); err != nil {
		return err
	}
	if yes, err = h.CalculateDeduction(); err != nil || yes {
		if err != nil {
			return err
		}
		if yes {
			h.resolveCreateOutOfGas()
			if h.createOutOfGas {
				h.feeEndAt = uint32(math.Min(float64(h.TechniqueFeeEndAt), float64(h.ElectricityFeeEndAt)))
				h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
			}
		}
	}
	return nil
}
