package executor

import (
	"context"
	"fmt"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/received/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	retry          chan interface{}
	persistent     chan interface{}
	notif          chan interface{}
	incomingAmount decimal.Decimal
	transferAmount decimal.Decimal
	paymentCoin    *coinmwpb.Coin
	paymentAccount *payaccmwpb.Account
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

func (h *orderHandler) getPaymentCoin(ctx context.Context) error {
	if !h.onlinePayment() || h.payWithBalanceOnly() {
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
	if !h.onlinePayment() || h.payWithBalanceOnly() {
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
	if !h.onlinePayment() || h.payWithBalanceOnly() {
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
	startAmount, err := decimal.NewFromString(h.PaymentStartAmount)
	if err != nil {
		return err
	}
	h.incomingAmount = bal.Sub(startAmount)
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"Error", *err,
			"IncomingAmount", h.incomingAmount,
			"TransferAmount", h.transferAmount,
			"paymentCoin", h.paymentCoin,
			"paymentAccount", h.paymentAccount,
			"Error", *err,
		)
	}

	persistentOrder := &types.PersistentOrder{
		Order:          h.Order,
		IncomingAmount: h.incomingAmount.String(),
		TransferAmount: h.transferAmount.String(),
		Error:          *err,
	}
	if h.incomingAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		persistentOrder.IncomingExtra = fmt.Sprintf(
			`{"PaymentID": "%v","OrderID":"%v","PaymentState":"%v","GoodID":"%v"}`,
			h.PaymentID,
			h.ID,
			h.PaymentState,
			h.GoodID,
		)
	}
	if h.transferAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		persistentOrder.TransferExtra = fmt.Sprintf(
			`{"PaymentID":"%v","OrderID": "%v","FromTransfer":true}`,
			h.PaymentID,
			h.ID,
		)
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
	} else {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
		retry1.Retry(ctx, h.Order, h.retry)
	}
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	if h.transferAmount, err = decimal.NewFromString(h.TransferAmount); err != nil {
		return err
	}

	defer h.final(ctx, &err)

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
