package executor

import (
	"context"
	"fmt"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/wait/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent           chan interface{}
	notif                chan interface{}
	done                 chan interface{}
	paymentTransferCoins map[string]*coinmwpb.Coin
	paymentAccounts      map[string]*payaccmwpb.Account
	newOrderState        ordertypes.OrderState
	newPaymentState      ordertypes.PaymentState
}

func (h *orderHandler) paymentNoPayment() bool {
	return len(h.PaymentTransfers) == 0 && len(h.PaymentBalances) == 0
}

func (h *orderHandler) payWithTransfer() bool {
	return len(h.PaymentTransfers) > 0
}

func (h *orderHandler) timeout() bool {
	const timeoutSeconds = 6 * timedef.SecondsPerHour
	return h.CreatedAt+timeoutSeconds < uint32(time.Now().Unix())
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

func (h *orderHandler) checkPaymentTransferBalance(ctx context.Context) error {
	for _, paymentTransfer := range h.PaymentTransfers {
		paymentCoin, _ := h.paymentTransferCoins[paymentTransfer.CoinTypeID]
		paymentAccount, _ := h.paymentAccounts[paymentTransfer.AccountID]

		balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    paymentCoin.Name,
			Address: paymentAccount.Address,
		})
		if err != nil {
			return err
		}
		if balance == nil {
			return fmt.Errorf("invalid balance")
		}
		bal, err := decimal.NewFromString(balance.BalanceStr)
		if err != nil {
			return err
		}
		startAmount, err := decimal.NewFromString(paymentTransfer.StartAmount)
		if err != nil {
			return err
		}
		amount, err := decimal.NewFromString(paymentTransfer.Amount)
		if err != nil {
			return err
		}
		if bal.Cmp(startAmount.Add(amount)) >= 0 {
			continue
		}
		return nil
	}
	// Here we have enough balance
	h.newOrderState = ordertypes.OrderState_OrderStatePaymentTransferReceived
	h.newPaymentState = ordertypes.PaymentState_PaymentStateDone
	return nil
}

func (h *orderHandler) preResolveNewState() bool {
	if h.timeout() {
		h.newOrderState = ordertypes.OrderState_OrderStatePaymentTimeout
		h.newPaymentState = ordertypes.PaymentState_PaymentStateTimeout
		return true
	}
	if h.paymentNoPayment() {
		h.newOrderState = ordertypes.OrderState_OrderStatePaymentTransferReceived
		if h.OrderType == ordertypes.OrderType_Offline {
			h.newPaymentState = ordertypes.PaymentState_PaymentStateDone
		}
		return true
	}
	return false
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"PaymentTransferCoins", h.paymentTransferCoins,
			"PaymentAccounts", h.paymentAccounts,
			"NewOrderState", h.newOrderState,
			"NewPaymentState", h.newPaymentState,
			"Error", *err,
		)
	}

	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder: h.PowerRentalOrder,
		NewOrderState:    h.newOrderState,
		Error:            *err,
	}
	if h.newPaymentState != h.PaymentState {
		persistentOrder.NewPaymentState = &h.newPaymentState
	}
	if h.newOrderState == h.OrderState && *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
		return
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	}
	if h.newOrderState != h.OrderState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.newOrderState = h.OrderState
	h.newPaymentState = h.PaymentState

	var err error

	defer h.final(ctx, &err)

	if h.preResolveNewState() {
		return nil
	}
	if err = h.getPaymentCoins(ctx); err != nil {
		return err
	}
	if err = h.getPaymentAccounts(ctx); err != nil {
		return err
	}
	if err = h.checkPaymentTransferBalance(ctx); err != nil {
		return err
	}
	return nil
}
