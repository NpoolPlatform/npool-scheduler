package executor

import (
	"context"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	currencymwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency"
	orderrenewpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order/renew"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/notify/types"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent      chan interface{}
	done            chan interface{}
	notif           chan interface{}
	newRenewState   ordertypes.OrderRenewState
	willCreateOrder bool
}

func (h *orderHandler) resolveRenewState() {
	if h.InsufficientBalance {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		return
	}
	now := uint32(time.Now().Unix())
	const createOrderSeconds = timedef.SecondsPerHour * 6
	if (h.CheckElectricityFee && h.ElectricityFeeEndAt <= now+createOrderSeconds) ||
		(h.CheckTechniqueFee && h.TechniqueFeeEndAt <= now+createOrderSeconds) {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewExecute
		h.willCreateOrder = true
		return
	}
	h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"newRenewState", h.newRenewState,
			"CheckElectricityFee", h.CheckElectricityFee,
			"ElectricityFeeUSDAmount", h.ElectricityFeeUSDAmount,
			"CheckTechniqueFee", h.CheckTechniqueFee,
			"TechniqueFeeUSDAmount", h.TechniqueFeeUSDAmount,
			"Deductions", h.Deductions,
			"InsufficientBalance", h.InsufficientBalance,
			"RenewInfos", h.RenewInfos,
			"willCreateOrder", h.willCreateOrder,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder: h.PowerRentalOrder,
		MsgOrderChildsRenewReq: &orderrenewpb.MsgOrderChildsRenewReq{
			ParentOrder: &orderrenewpb.OrderInfo{
				OrderID:  h.OrderID,
				GoodType: h.GoodType,
			},
			Deductions:          h.Deductions,
			InsufficientBalance: h.InsufficientBalance,
			WillCreateOrder:     h.willCreateOrder,
			RenewInfos:          h.RenewInfos,
		},
		NewRenewState: h.newRenewState,
	}
	if *err != nil {
		errStr := (*err).Error()
		persistentOrder.MsgOrderChildsRenewReq.Error = &errStr
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	if h.newRenewState != h.RenewState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.newRenewState = h.RenewState
	h.Currencies = map[string]*currencymwpb.Currency{}

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
	if yes, err = h.Renewable(ctx); err != nil || !yes {
		return err
	}
	if err = h.CalculateRenewDuration(ctx); err != nil {
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
	if err = h.CalculateUSDAmount(); err != nil {
		return err
	}
	if _, err = h.CalculateDeduction(); err != nil { // yes means insufficient balance
		return err
	}
	h.resolveRenewState()

	return nil
}
