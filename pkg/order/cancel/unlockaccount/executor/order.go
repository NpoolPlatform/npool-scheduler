package executor

import (
	"context"
	"fmt"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/unlockaccount/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent     chan interface{}
	notif          chan interface{}
	done           chan interface{}
	paymentAccount *payaccmwpb.Account
	transferAmount decimal.Decimal
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

func (h *orderHandler) checkUnlockable() bool {
	if !h.onlinePayment() || h.payWithBalanceOnly() {
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

func (h *orderHandler) checkPaymentAccount(ctx context.Context) error {
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

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"PaymentAccount", h.paymentAccount,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	if h.paymentAccount != nil {
		persistentOrder.OrderPaymentAccountID = &h.paymentAccount.ID
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

func (h *orderHandler) exec(ctx context.Context) error { //nolint
	var err error

	defer h.final(ctx, &err)

	if h.transferAmount, err = decimal.NewFromString(h.TransferAmount); err != nil {
		return err
	}
	if able := h.checkUnlockable(); !able {
		return nil
	}
	if err = accountlock.Lock(h.PaymentAccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(h.PaymentAccountID) //nolint
	}()

	if err = h.checkPaymentAccount(ctx); err != nil {
		return err
	}

	return nil
}
