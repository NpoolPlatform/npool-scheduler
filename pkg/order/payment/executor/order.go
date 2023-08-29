package executor

import (
	"context"
	"fmt"
	"time"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	good                  *appgoodmwpb.Good
	paymentCoin           *coinmwpb.Coin
	paymentAccount        *payaccmwpb.Account
	paymentAccountBalance decimal.Decimal
	newOrderState         ordertypes.OrderState
	newPaymentState       ordertypes.PaymentState
	remainBalance         decimal.Decimal
	retry                 chan interface{}
	persistent            chan interface{}
	notif                 chan interface{}
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
	return true
}

func (h *orderHandler) payWithBalanceOnly() bool {
	return h.PaymentType == ordertypes.PaymentType_PayWithBalanceOnly
}

func (h *orderHandler) timeout() bool {
	const timeoutSeconds = 6 * timedef.SecondsPerHour
	return h.CreatedAt+timeoutSeconds < uint32(time.Now().Unix())
}

func (h *orderHandler) canceled() bool {
	return h.UserSetCanceled
}

func (h *orderHandler) getPaymentCoin(ctx context.Context) error {
	if !h.onlinePayment() || h.timeout() || h.canceled() {
		return nil
	}

	coin, err := coinmwcli.GetCoin(ctx, h.PaymentCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	if !coin.ForPay {
		return fmt.Errorf("invalid payment coin")
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
	return nil
}

func (h *orderHandler) paymentBalanceEnough() (bool, error) {
	transferAmount, err := decimal.NewFromString(h.TransferAmount)
	if err != nil {
		return false, err
	}
	startAmount, err := decimal.NewFromString(h.PaymentStartAmount)
	if err != nil {
		return false, err
	}
	return h.paymentAccountBalance.Sub(transferAmount).Sub(startAmount).Cmp(decimal.NewFromInt(0)) >= 0, nil
}

func (h *orderHandler) orderStatePaymentRemain() (decimal.Decimal, error) {
	transferAmount, err := decimal.NewFromString(h.TransferAmount)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	startAmount, err := decimal.NewFromString(h.PaymentStartAmount)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if h.paymentAccountBalance.Cmp(startAmount) < 0 {
		return decimal.NewFromInt(0), fmt.Errorf("invalid balance")
	}
	if h.canceled() || h.timeout() {
		return h.paymentAccountBalance.Sub(startAmount), nil
	}
	if h.newOrderState == ordertypes.OrderState_OrderStatePaid {
		return h.paymentAccountBalance.Sub(startAmount).Sub(transferAmount), nil
	}
	return decimal.NewFromInt(0), nil
}

func (h *orderHandler) resolveNewState() error {
	if h.canceled() {
		h.newOrderState = ordertypes.OrderState_OrderStateCanceled
		h.newPaymentState = ordertypes.PaymentState_PaymentStateCanceled
		return nil
	}
	if h.timeout() {
		h.newOrderState = ordertypes.OrderState_OrderStatePaymentTimeout
		h.newPaymentState = ordertypes.PaymentState_PaymentStateTimeout
		return nil
	}
	if !h.onlinePayment() {
		h.newOrderState = ordertypes.OrderState_OrderStatePaid
		h.newPaymentState = ordertypes.PaymentState_PaymentStateDone
		return nil
	}
	enough, err := h.paymentBalanceEnough()
	if err != nil {
		return err
	}
	if enough {
		h.newOrderState = ordertypes.OrderState_OrderStatePaid
		h.newPaymentState = ordertypes.PaymentState_PaymentStateDone
	}
	return nil
}

func (h *orderHandler) final(ctx context.Context, err *error) {
	// Update order state
	// Move good stock from lock to paid or spot quantity
	// Change lock state of payment account
	// Update user ledger and statement (incoming, outcoming, locked balance)
	// Update user achievement and statement
	// Allocate reward of user purchase action
	// Send order payment notification or timeout hint

	persistentOrder := &types.PersistentOrder{
		Order:           h.Order,
		PaymentBalance:  h.paymentAccountBalance,
		NewOrderState:   h.newOrderState,
		NewPaymentState: h.newPaymentState,
		Error:           *err,
	}

	h.notifOrder <- persistentOrder
	if h.newOrderState != h.OrderState && *err == nil {
		h.persistentOrder <- persistentOrder
		return
	}

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			h.retryOrder <- h.Order
		}
	}()
}

func (h *orderHandler) exec(ctx context.Context) error {
	h.newOrderState = h.OrderState
	h.newPaymentState = h.PaymentState

	var err error
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
		_ = accountlock.Unlock(h.PaymentAccountID)
	}()

	if err = h.getPaymentAccount(ctx); err != nil {
		return err
	}
	if err = h.getPaymentAccountBalance(ctx); err != nil {
		return err
	}
	if err = h.resolveNewState(); err != nil {
		return err
	}
	_, err = h.orderStatePaymentRemain()
	if err != nil {
		return err
	}

	return nil
}
