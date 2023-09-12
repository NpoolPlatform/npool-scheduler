package executor

import (
	"context"
	"fmt"
	"time"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/wait/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent            chan interface{}
	notif                 chan interface{}
	done                  chan interface{}
	good                  *appgoodmwpb.Good
	paymentCoin           *coinmwpb.Coin
	paymentAccount        *payaccmwpb.Account
	paymentAccountBalance decimal.Decimal
	incomingAmount        decimal.Decimal
	transferAmount        decimal.Decimal
	newOrderState         ordertypes.OrderState
	newPaymentState       ordertypes.PaymentState
}

func (h *orderHandler) getGood(ctx context.Context) error {
	if h.timeout() || h.canceled() {
		return nil
	}

	good, err := appgoodmwcli.GetGoodOnly(ctx, &appgoodmwpb.Conds{
		AppID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
		ID:     &basetypes.StringVal{Op: cruder.EQ, Value: h.AppGoodID},
	})
	if err != nil {
		return err
	}
	if good == nil {
		return fmt.Errorf("invalid good")
	}
	return nil
}

func (h *orderHandler) onlinePayment() bool {
	switch h.OrderType {
	case ordertypes.OrderType_Offline:
		fallthrough //nolint
	case ordertypes.OrderType_Airdrop:
		return false
	}
	return h.transferAmount.Cmp(decimal.NewFromInt(0)) > 0
}

func (h *orderHandler) payWithBalanceOnly() bool {
	return h.PaymentType == ordertypes.PaymentType_PayWithBalanceOnly
}

func (h *orderHandler) timeout() bool {
	const timeoutSeconds = 6 * timedef.SecondsPerHour
	return h.CreatedAt+timeoutSeconds < uint32(time.Now().Unix())
}

func (h *orderHandler) canceled() bool {
	return h.UserSetCanceled || h.AdminSetCanceled
}

func (h *orderHandler) getPaymentCoin(ctx context.Context) error {
	if !h.onlinePayment() || h.payWithBalanceOnly() || h.timeout() || h.canceled() {
		return nil
	}

	coin, err := coinmwcli.GetCoin(ctx, h.PaymentCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid payment coin")
	}
	if !coin.ForPay {
		return fmt.Errorf("coin not payable")
	}
	h.paymentCoin = coin
	return nil
}

func (h *orderHandler) getPaymentAccount(ctx context.Context) error {
	if !h.onlinePayment() || h.payWithBalanceOnly() || h.timeout() || h.canceled() {
		return nil
	}

	account, err := payaccmwcli.GetAccountOnly(ctx, &payaccmwpb.Conds{
		AccountID: &basetypes.StringVal{Op: cruder.EQ, Value: h.PaymentAccountID},
		Active:    &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:    &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		LockedBy:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountLockedBy_Payment)},
		Blocked:   &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid account")
	}
	h.paymentAccount = account
	return nil
}

func (h *orderHandler) getPaymentAccountBalance(ctx context.Context) error {
	if !h.onlinePayment() || h.payWithBalanceOnly() || h.timeout() || h.canceled() {
		return nil
	}
	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.paymentCoin.Name,
		Address: h.paymentAccount.Address,
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
	h.paymentAccountBalance = bal
	startAmount, err := decimal.NewFromString(h.PaymentStartAmount)
	if err != nil {
		return err
	}
	h.incomingAmount = h.paymentAccountBalance.Sub(startAmount)
	return nil
}

func (h *orderHandler) paymentBalanceEnough() bool {
	return h.incomingAmount.Sub(h.transferAmount).Cmp(decimal.NewFromInt(0)) >= 0
}

func (h *orderHandler) resolveNewState() {
	if h.canceled() {
		h.newOrderState = ordertypes.OrderState_OrderStatePreCancel
		h.newPaymentState = ordertypes.PaymentState_PaymentStateCanceled
		return
	}
	if h.timeout() {
		h.newOrderState = ordertypes.OrderState_OrderStatePaymentTimeout
		h.newPaymentState = ordertypes.PaymentState_PaymentStateTimeout
		return
	}
	if !h.onlinePayment() {
		h.newOrderState = ordertypes.OrderState_OrderStatePaymentTransferReceived
		h.newPaymentState = ordertypes.PaymentState_PaymentStateDone
		return
	}
	if h.paymentBalanceEnough() {
		h.newOrderState = ordertypes.OrderState_OrderStatePaymentTransferReceived
		h.newPaymentState = ordertypes.PaymentState_PaymentStateDone
	}
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil || true {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"Good", h.good,
			"PaymentCoin", h.paymentCoin,
			"PaymentAccount", h.paymentAccount,
			"PaymentAccountBalance", h.paymentAccountBalance,
			"IncomingAmount", h.incomingAmount,
			"TransferAmount", h.transferAmount,
			"NewOrderState", h.newOrderState,
			"NewPaymentState", h.newPaymentState,
			"Error", *err,
		)
	}

	persistentOrder := &types.PersistentOrder{
		Order:           h.Order,
		NewOrderState:   h.newOrderState,
		NewPaymentState: h.newPaymentState,
		Error:           *err,
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
	if h.transferAmount, err = decimal.NewFromString(h.TransferAmount); err != nil {
		return err
	}

	defer h.final(ctx, &err)

	if err = h.getGood(ctx); err != nil {
		return err
	}
	if err = h.getPaymentCoin(ctx); err != nil {
		return err
	}

	if err = accountlock.Lock(h.PaymentAccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(h.PaymentAccountID) //nolint
	}()

	if err = h.getPaymentAccount(ctx); err != nil {
		return err
	}
	if err = h.getPaymentAccountBalance(ctx); err != nil {
		return err
	}
	h.resolveNewState()
	return nil
}
