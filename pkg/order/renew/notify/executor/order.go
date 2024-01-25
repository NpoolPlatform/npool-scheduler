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
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/notify/types"
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
	if h.ElectricityFeeEndAt <= now+createOrderSeconds {
		h.newRenewState = ordertypes.OrderRenewState_OrderRenewExecute
		h.willCreateOrder = true
	}
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
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
		Order: h.Order,
		MsgOrderChildsRenewReq: &orderrenewpb.MsgOrderChildsRenewReq{
			ParentOrder:         h.Order,
			Deductions:          h.Deductions,
			InsufficientBalance: h.InsufficientBalance,
			WillCreateOrder:     h.willCreateOrder,
			RenewInfos:          h.RenewInfos,
		},
		NewRenewState: h.newRenewState,
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	if h.newRenewState != h.RenewState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.newRenewState = h.RenewState
	h.Currencies = map[string]*currencymwpb.Currency{}

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
