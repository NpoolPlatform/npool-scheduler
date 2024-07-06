//nolint:dupl
package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	outofgasmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/outofgas"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/execute/types"
	outofgasmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/outofgas"

	"github.com/google/uuid"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent    chan interface{}
	done          chan interface{}
	notif         chan interface{}
	newRenewState ordertypes.OrderRenewState
	feeOrderReqs  []*feeordermwpb.FeeOrderReq
	paymentID     *string
	ledgerLockID  string
	outOfGasEntID *string
}

func (h *orderHandler) formalizePayment(req *feeordermwpb.FeeOrderReq) {
	if h.paymentID == nil {
		req.PaymentType = func() *ordertypes.PaymentType { e := ordertypes.PaymentType_PayWithBalanceOnly; return &e }()
		req.PaymentID = func() *string { s := uuid.NewString(); return &s }()
		h.paymentID = req.PaymentID
		req.LedgerLockID = func() *string { s := uuid.NewString(); return &s }()
		h.ledgerLockID = *req.LedgerLockID
		for _, deduction := range h.Deductions {
			req.PaymentBalances = append(req.PaymentBalances, &paymentmwpb.PaymentBalanceReq{
				CoinTypeID:           &deduction.AppCoin.CoinTypeID,
				Amount:               &deduction.Amount,
				CoinUSDCurrency:      &deduction.USDCurrency,
				LocalCoinUSDCurrency: &deduction.USDCurrency,
				LiveCoinUSDCurrency:  &deduction.USDCurrency,
			})
		}
		req.PaymentAmountUSD = func() *string { s := h.ElectricityFeeUSDAmount.Add(h.TechniqueFeeUSDAmount).String(); return &s }()
	} else {
		req.PaymentType = func() *ordertypes.PaymentType { e := ordertypes.PaymentType_PayWithOtherOrder; return &e }()
		req.PaymentID = h.paymentID
	}
}

func (h *orderHandler) constructElectricityFeeOrder() {
	if !h.CheckElectricityFee {
		return
	}
	req := &feeordermwpb.FeeOrderReq{
		AppID:           &h.AppID,
		UserID:          &h.UserID,
		GoodID:          &h.ElectricityFee.GoodID,
		GoodType:        &h.ElectricityFee.GoodType,
		AppGoodID:       &h.ElectricityFee.AppGoodID,
		ParentOrderID:   &h.OrderID,
		OrderType:       func() *ordertypes.OrderType { e := ordertypes.OrderType_Normal; return &e }(),
		CreateMethod:    func() *ordertypes.OrderCreateMethod { e := ordertypes.OrderCreateMethod_OrderCreatedByRenew; return &e }(),
		GoodValueUSD:    func() *string { s := h.ElectricityFeeUSDAmount.String(); return &s }(),
		DurationSeconds: &h.ElectricityFeeExtendSeconds,
	}
	h.formalizePayment(req)
	h.feeOrderReqs = append(h.feeOrderReqs, req)
}

func (h *orderHandler) constructTechniqueFeeOrder() {
	if !h.CheckTechniqueFee {
		return
	}
	req := &feeordermwpb.FeeOrderReq{
		AppID:           &h.AppID,
		UserID:          &h.UserID,
		GoodID:          &h.TechniqueFee.GoodID,
		GoodType:        &h.TechniqueFee.GoodType,
		AppGoodID:       &h.TechniqueFee.AppGoodID,
		ParentOrderID:   &h.OrderID,
		OrderType:       func() *ordertypes.OrderType { e := ordertypes.OrderType_Normal; return &e }(),
		CreateMethod:    func() *ordertypes.OrderCreateMethod { e := ordertypes.OrderCreateMethod_OrderCreatedByRenew; return &e }(),
		GoodValueUSD:    func() *string { s := h.TechniqueFeeUSDAmount.String(); return &s }(),
		DurationSeconds: &h.TechniqueFeeExtendSeconds,
	}
	h.formalizePayment(req)
	h.feeOrderReqs = append(h.feeOrderReqs, req)
}

func (h *orderHandler) constructRenewOrders() {
	h.constructElectricityFeeOrder()
	h.constructTechniqueFeeOrder()
	h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
}

func (h *orderHandler) getOutOfGas(ctx context.Context) error {
	info, err := outofgasmwcli.GetOutOfGasOnly(ctx, &outofgasmwpb.Conds{
		OrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.OrderID},
		EndAt:   &basetypes.Uint32Val{Op: cruder.EQ, Value: 0},
	})
	if err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	h.outOfGasEntID = &info.EntID
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"newRenewState", h.newRenewState,
			"FeeOrderReqs", h.feeOrderReqs,
			"TechniqueFeeEndAt", h.TechniqueFeeEndAt,
			"TechniqueFeeDuration", h.TechniqueFeeSeconds,
			"StartAt", h.StartAt,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder:    h.PowerRentalOrder,
		InsufficientBalance: h.InsufficientBalance,
		FeeOrderReqs:        h.feeOrderReqs,
		NewRenewState:       h.newRenewState,
		LedgerLockID:        h.ledgerLockID,
		OutOfGasEntID:       h.outOfGasEntID,
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
	h.FormalizeFeeDurationSeconds()
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
	if err = h.getOutOfGas(ctx); err != nil {
		return err
	}
	if yes, err = h.CalculateDeduction(); err != nil || yes {
		if yes {
			h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
		}
		return err
	}
	h.constructRenewOrders()

	return nil
}
