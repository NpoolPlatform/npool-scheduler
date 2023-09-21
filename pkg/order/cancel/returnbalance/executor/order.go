package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/returnbalance/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent          chan interface{}
	notif               chan interface{}
	done                chan interface{}
	lockedBalanceAmount decimal.Decimal
	spentBalanceAmount  decimal.Decimal
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"LockedBalance", h.lockedBalanceAmount,
			"SpentBalance", h.spentBalanceAmount,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	if h.lockedBalanceAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := h.lockedBalanceAmount.String()
		persistentOrder.LockedBalanceAmount = &amount
	}
	if h.spentBalanceAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := h.spentBalanceAmount.String()
		persistentOrder.SpentAmount = &amount
		ioExtra := fmt.Sprintf(
			`{"AppID":"%v","UserID":"%v","OrderID":"%v","Amount":"%v","CancelOrder":true}`,
			h.AppID,
			h.UserID,
			h.ID,
			h.spentBalanceAmount,
		)
		persistentOrder.SpentExtra = ioExtra
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

	switch h.OrderType {
	case ordertypes.OrderType_Offline:
		fallthrough //nolint
	case ordertypes.OrderType_Airdrop:
		return nil
	}

	switch h.CancelState {
	case ordertypes.OrderState_OrderStateWaitPayment:
		fallthrough //nolint
	case ordertypes.OrderState_OrderStatePaymentTimeout:
		h.lockedBalanceAmount, err = decimal.NewFromString(h.BalanceAmount)
		if err != nil {
			return err
		}
	case ordertypes.OrderState_OrderStatePaid:
		fallthrough //nolint
	case ordertypes.OrderState_OrderStateInService:
		h.spentBalanceAmount, err = decimal.NewFromString(h.PaymentAmount)
		if err != nil {
			return err
		}
	}

	return nil
}
