package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/cancel/bookkeeping/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*feeordermwpb.FeeOrder
	persistent           chan interface{}
	notif                chan interface{}
	done                 chan interface{}
	paymentTransfers     []*types.XPaymentTransfer
	paymentTransferCoins map[string]*coinmwpb.Coin
	paymentAccounts      map[string]*paymentaccountmwpb.Account
}

func (h *orderHandler) payWithTransfer() bool {
	return len(h.PaymentTransfers) > 0
}

func (h *orderHandler) checkBookKeepingAble() bool {
	if !h.payWithTransfer() {
		return false
	}
	switch h.CancelState {
	case ordertypes.OrderState_OrderStateWaitPayment:
	case ordertypes.OrderState_OrderStatePaymentTimeout:
	default:
		return false
	}
	return true
}

func (h *orderHandler) constructPaymentTransfers() error {
	for _, paymentTransfer := range h.PaymentTransfers {
		amount, err := decimal.NewFromString(paymentTransfer.Amount)
		if err != nil {
			return wlog.WrapError(err)
		}
		if amount.Cmp(decimal.NewFromInt(0)) <= 0 {
			return wlog.Errorf("invalid paymentamount")
		}
		startAmount, err := decimal.NewFromString(paymentTransfer.StartAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		h.paymentTransfers = append(h.paymentTransfers, &types.XPaymentTransfer{
			PaymentTransferID: paymentTransfer.EntID,
			CoinTypeID:        paymentTransfer.CoinTypeID,
			AccountID:         paymentTransfer.AccountID,
			Amount:            amount,
			StartAmount:       startAmount,
		})
	}
	return nil
}

func (h *orderHandler) getPaymentCoins(ctx context.Context) (err error) {
	h.paymentTransferCoins, err = schedcommon.GetCoins(ctx, func() (coinTypeIDs []string) {
		for _, paymentTransfer := range h.PaymentTransfers {
			coinTypeIDs = append(coinTypeIDs, paymentTransfer.CoinTypeID)
		}
		return
	}())
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, paymentTransfer := range h.PaymentTransfers {
		if _, ok := h.paymentTransferCoins[paymentTransfer.CoinTypeID]; !ok {
			return wlog.Errorf("invalid paymenttransfercoin")
		}
	}
	for _, paymentCoin := range h.paymentTransferCoins {
		if !paymentCoin.ForPay {
			return wlog.Errorf("invalid paymenttransfercoin")
		}
	}
	return nil
}

func (h *orderHandler) getPaymentAccounts(ctx context.Context) (err error) {
	h.paymentAccounts, err = schedcommon.GetPaymentAccounts(ctx, func() (accountIDs []string) {
		for _, paymentTransfer := range h.PaymentTransfers {
			accountIDs = append(accountIDs, paymentTransfer.AccountID)
		}
		return
	}())
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, paymentTransfer := range h.PaymentTransfers {
		if _, ok := h.paymentAccounts[paymentTransfer.AccountID]; !ok {
			return wlog.Errorf("invalid paymentaccount")
		}
	}
	return nil
}

func (h *orderHandler) updatePaymentTransfers(ctx context.Context) error {
	for _, paymentTransfer := range h.paymentTransfers {
		paymentCoin, ok := h.paymentTransferCoins[paymentTransfer.CoinTypeID]
		if !ok {
			return wlog.Errorf("invalid paymentcoin")
		}
		paymentAccount, ok := h.paymentAccounts[paymentTransfer.AccountID]
		if !ok {
			return wlog.Errorf("invalid paymentaccount")
		}

		balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    paymentCoin.Name,
			Address: paymentAccount.Address,
		})
		if err != nil {
			return wlog.WrapError(err)
		}
		if balance == nil {
			return wlog.Errorf("invalid balance")
		}

		bal, err := decimal.NewFromString(balance.BalanceStr)
		if err != nil {
			return wlog.WrapError(err)
		}
		paymentTransfer.IncomingAmount = func() *string {
			amount := bal.Sub(paymentTransfer.StartAmount)
			if amount.Cmp(decimal.NewFromInt(0)) <= 0 {
				return nil
			}
			s := amount.String()
			return &s
		}()
		paymentTransfer.FinishAmount = balance.BalanceStr
	}
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.FeeOrder,
			"PaymentTransferCoins", h.paymentTransferCoins,
			"PaymentAccounts", h.paymentAccounts,
			"PaymentTransfers", h.paymentTransfers,
			"Error", *err,
		)
	}
	persistentFeeOrder := &types.PersistentFeeOrder{
		FeeOrder:          h.FeeOrder,
		XPaymentTransfers: h.paymentTransfers,
	}
	persistentFeeOrder.XPaymentTransfers = h.paymentTransfers
	if len(h.paymentTransfers) > 0 {
		ioExtra := fmt.Sprintf(
			`{"AppID":"%v","UserID":"%v","OrderID":"%v","CancelOrder":true}`,
			h.AppID,
			h.UserID,
			h.OrderID,
		)
		persistentFeeOrder.IncomingExtra = ioExtra
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentFeeOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentFeeOrder, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentFeeOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if able := h.checkBookKeepingAble(); !able {
		return nil
	}
	if err = h.constructPaymentTransfers(); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.getPaymentCoins(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.getPaymentAccounts(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.updatePaymentTransfers(ctx); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
