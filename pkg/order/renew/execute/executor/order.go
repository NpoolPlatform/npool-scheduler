//nolint:dupl
package executor

import (
	"context"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
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
	orderReqs     []*types.OrderReq
}

func (h *orderHandler) constructElectricityFeeOrder() {
	if !h.CheckElectricityFee {
		return
	}

	balances := []*ledgermwpb.LockBalancesRequest_XBalance{}
	amounts := []*ordermwpb.PaymentAmount{}

	for _, deduction := range h.Deductions {
		if deduction.AppGood.EntID == h.ElectricityFeeAppGood.EntID {
			balances = append(balances, &ledgermwpb.LockBalancesRequest_XBalance{
				CoinTypeID: deduction.AppCoin.CoinTypeID,
				Amount:     deduction.Amount,
			})
			amounts = append(amounts, &ordermwpb.PaymentAmount{
				CoinTypeID:  deduction.AppCoin.CoinTypeID,
				USDCurrency: deduction.USDCurrency,
				Amount:      deduction.Amount,
			})
		}
	}

	multiPaymentCoins := true
	usdAmount := h.ElectricityFeeUSDAmount.String()
	orderType := ordertypes.OrderType_Normal
	paymentType := ordertypes.PaymentType_PayWithBalanceOnly
	createMethod := ordertypes.OrderCreateMethod_OrderCreatedByRenew
	investmentType := ordertypes.InvestmentType_FullPayment

	startAt := uint32(time.Now().Unix() + 10*timedef.SecondsPerMinute)
	if startAt < h.ElectricityFeeEndAt {
		startAt = h.ElectricityFeeEndAt
	}
	endAt := startAt + h.ElectricityFeeExtendSeconds
	if endAt > h.EndAt {
		endAt = h.EndAt
	}

	orderReq := &ordermwpb.OrderReq{
		AppID:             &h.AppID,
		UserID:            &h.UserID,
		GoodID:            &h.ElectricityFeeAppGood.GoodID,
		AppGoodID:         &h.ElectricityFeeAppGood.EntID,
		ParentOrderID:     &h.EntID,
		Units:             &h.Units,
		GoodValue:         &usdAmount,
		GoodValueUSD:      &usdAmount,
		Duration:          &h.ElectricityFeeExtendDuration,
		OrderType:         &orderType,
		PaymentType:       &paymentType,
		CreateMethod:      &createMethod,
		MultiPaymentCoins: &multiPaymentCoins,
		PaymentAmounts:    amounts,
		InvestmentType:    &investmentType,
		CoinTypeID:        &h.ElectricityFeeAppGood.CoinTypeID,
		StartAt:           &startAt,
		EndAt:             &endAt,
	}

	h.orderReqs = append(h.orderReqs, &types.OrderReq{
		OrderReq: orderReq,
		Balances: balances,
	})
}

func (h *orderHandler) constructTechniqueFeeOrder() {
	if !h.CheckTechniqueFee {
		return
	}

	balances := []*ledgermwpb.LockBalancesRequest_XBalance{}
	amounts := []*ordermwpb.PaymentAmount{}

	for _, deduction := range h.Deductions {
		if deduction.AppGood.EntID == h.TechniqueFeeAppGood.EntID {
			balances = append(balances, &ledgermwpb.LockBalancesRequest_XBalance{
				CoinTypeID: deduction.AppCoin.CoinTypeID,
				Amount:     deduction.Amount,
			})
			amounts = append(amounts, &ordermwpb.PaymentAmount{
				CoinTypeID:  deduction.AppCoin.CoinTypeID,
				USDCurrency: deduction.USDCurrency,
				Amount:      deduction.Amount,
			})
		}
	}

	multiPaymentCoins := true
	usdAmount := h.TechniqueFeeUSDAmount.String()
	orderType := ordertypes.OrderType_Normal
	paymentType := ordertypes.PaymentType_PayWithBalanceOnly
	createMethod := ordertypes.OrderCreateMethod_OrderCreatedByRenew
	investmentType := ordertypes.InvestmentType_FullPayment

	startAt := uint32(time.Now().Unix() + 10*timedef.SecondsPerMinute)
	if startAt < h.TechniqueFeeEndAt {
		startAt = h.TechniqueFeeEndAt
	}
	endAt := startAt + h.TechniqueFeeExtendSeconds
	if endAt > h.EndAt {
		endAt = h.EndAt
	}

	orderReq := &ordermwpb.OrderReq{
		AppID:             &h.AppID,
		UserID:            &h.UserID,
		GoodID:            &h.TechniqueFeeAppGood.GoodID,
		AppGoodID:         &h.TechniqueFeeAppGood.EntID,
		ParentOrderID:     &h.EntID,
		Units:             &h.Units,
		GoodValue:         &usdAmount,
		GoodValueUSD:      &usdAmount,
		Duration:          &h.TechniqueFeeExtendDuration,
		OrderType:         &orderType,
		PaymentType:       &paymentType,
		CreateMethod:      &createMethod,
		MultiPaymentCoins: &multiPaymentCoins,
		PaymentAmounts:    amounts,
		InvestmentType:    &investmentType,
		CoinTypeID:        &h.TechniqueFeeAppGood.CoinTypeID,
		StartAt:           &startAt,
		EndAt:             &endAt,
	}

	h.orderReqs = append(h.orderReqs, &types.OrderReq{
		OrderReq: orderReq,
		Balances: balances,
	})
}

func (h *orderHandler) constructRenewOrders() {
	h.constructElectricityFeeOrder()
	h.constructTechniqueFeeOrder()
	h.newRenewState = ordertypes.OrderRenewState_OrderRenewWait
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"newRenewState", h.newRenewState,
			"orderReqs", h.orderReqs,
			"TechniqueFeeEndAt", h.TechniqueFeeEndAt,
			"TechniqueFeeDuration", h.TechniqueFeeDuration,
			"StartAt", h.StartAt,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:         h.Order,
		OrderReqs:     h.orderReqs,
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
	if _, err = h.CalculateDeductionForOrder(); err != nil { // yes means insufficient balance
		return err
	}
	h.constructRenewOrders()

	return nil
}
