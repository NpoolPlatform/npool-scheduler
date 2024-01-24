package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	currencymwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/notify/types"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent    chan interface{}
	done          chan interface{}
	notif         chan interface{}
	newRenewState ordertypes.OrderRenewState
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil || true {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"newRenewState", h.newRenewState,
			"CheckElectricityFee", h.CheckElectricityFee,
			"ElectricityFeeUSDAmount", h.ElectricityFeeUSDAmount,
			"CheckTechniqueFee", h.CheckTechniqueFee,
			"TechniqueFeeUSDAmount", h.TechniqueFeeUSDAmount,
			"FeeDeductions", h.FeeDeductions,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:         h.Order,
		NewRenewState: h.newRenewState,
		FeeDeductions: h.FeeDeductions,
	}
	if *err != nil {
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
	if err = h.GetFeeDeductionCoins(ctx); err != nil {
		return err
	}
	if err = h.GetUserLedgers(ctx); err != nil {
		return err
	}
	if err = h.GetCoinUSDCurrency(ctx); err != nil {
		return err
	}
	if err = h.CalculateFeeUSDAmount(); err != nil {
		return err
	}
	if err = h.CalculateFeeDeduction(); err != nil {
		return err
	}
	return nil
}
