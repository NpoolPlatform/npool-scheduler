package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/check/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent    chan interface{}
	done          chan interface{}
	notif         chan interface{}
	newOrderState ordertypes.OrderState
}

func (h *orderHandler) expired() (bool, error) {
	switch h.PaymentState {
	case ordertypes.PaymentState_PaymentStateWait:
		fallthrough // nolint
	case ordertypes.PaymentState_PaymentStateCanceled:
		fallthrough // nolint
	case ordertypes.PaymentState_PaymentStateTimeout:
		return false, nil
	case ordertypes.PaymentState_PaymentStateDone:
	case ordertypes.PaymentState_PaymentStateNoPayment:
	default:
		return false, fmt.Errorf("invalid paymentstate")
	}
	if h.EndAt >= uint32(time.Now().Unix()) {
		return false, nil
	}
	h.newOrderState = ordertypes.OrderState_OrderStatePreExpired
	return true, nil
}

func (h *orderHandler) checkCanceled() bool {
	if h.AdminSetCanceled || h.UserSetCanceled {
		h.newOrderState = ordertypes.OrderState_OrderStatePreCancel
		return true
	}
	return false
}

func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"NewOrderState", h.newOrderState,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:         h.Order,
		NewOrderState: h.newOrderState,
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, h.Order, h.notif)
	}
	if h.newOrderState != h.OrderState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

func (h *orderHandler) exec(ctx context.Context) error {
	h.newOrderState = h.OrderState

	var err error
	var yes bool
	defer h.final(ctx, &err)

	if yes = h.checkCanceled(); yes {
		return nil
	}
	if yes, err = h.expired(); err != nil || !yes {
		return err
	}
	return nil
}
