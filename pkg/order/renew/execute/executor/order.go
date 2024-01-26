package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	orderrenewpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order/renew"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/execute/types"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent    chan interface{}
	done          chan interface{}
	notif         chan interface{}
	newRenewState ordertypes.OrderRenewState
	orderReqs     []*ordermwpb.OrderReq
}

func (h *orderHandler) constructRenewOrders() error {
	return nil
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
			"Deductions", h.Deductions,
			"InsufficientBalance", h.InsufficientBalance,
			"RenewInfos", h.RenewInfos,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
		MsgOrderChildsRenewReq: &orderrenewpb.MsgOrderChildsRenewReq{
			ParentOrder:         h.Order,
			Deductions:          h.Deductions,
			InsufficientBalance: h.InsufficientBalance,
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
	if err = h.constructRenewOrders(); err != nil {
		return err
	}

	return nil
}
