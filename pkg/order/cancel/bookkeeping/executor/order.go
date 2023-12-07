package executor

import (
	"context"
	"fmt"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/bookkeeping/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent            chan interface{}
	notif                 chan interface{}
	done                  chan interface{}
	paymentCoin           *coinmwpb.Coin
	paymentAccount        *payaccmwpb.Account
	paymentAccountBalance decimal.Decimal
	incomingAmount        decimal.Decimal
	transferAmount        decimal.Decimal
	bookKeepingAble       bool
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

func (h *orderHandler) checkBookKeepingAble() bool {
	if !h.onlinePayment() || h.payWithBalanceOnly() {
		return false
	}
	switch h.CancelState {
	case ordertypes.OrderState_OrderStateWaitPayment:
	case ordertypes.OrderState_OrderStatePaymentTimeout:
	default:
		return false
	}
	h.bookKeepingAble = true
	return true
}

func (h *orderHandler) getPaymentCoin(ctx context.Context) error {
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

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"PaymentCoin", h.paymentCoin,
			"PaymentAccount", h.paymentAccount,
			"PaymentAccountBalance", h.paymentAccountBalance,
			"IncomingAmount", h.incomingAmount,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	if h.bookKeepingAble {
		amount := h.paymentAccountBalance.String()
		persistentOrder.PaymentAccountBalance = &amount
	}
	if h.incomingAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := h.incomingAmount.String()
		persistentOrder.IncomingAmount = &amount
		ioExtra := fmt.Sprintf(
			`{"AppID":"%v","UserID":"%v","OrderID":"%v","Amount":"%v","CancelOrder":true}`,
			h.AppID,
			h.UserID,
			h.EntID,
			h.incomingAmount,
		)
		persistentOrder.IncomingExtra = ioExtra
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if h.transferAmount, err = decimal.NewFromString(h.TransferAmount); err != nil {
		return err
	}
	if able := h.checkBookKeepingAble(); !able {
		return nil
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

	return nil
}
