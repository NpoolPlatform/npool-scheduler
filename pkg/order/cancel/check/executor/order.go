package executor

import (
	"context"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/check/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent      chan interface{}
	newPaymentState *ordertypes.PaymentState
}

func (h *orderHandler) resolveNewPaymentState() {
	if h.OrderState == ordertypes.OrderState_OrderStateWaitPayment {
		state := ordertypes.PaymentState_PaymentStateCanceled
		h.newPaymentState = &state
	}
}

func (h *orderHandler) final(ctx context.Context) {
	persistentOrder := &types.PersistentOrder{
		Order:           h.Order,
		NewPaymentState: h.newPaymentState,
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
}

func (h *orderHandler) exec(ctx context.Context) error {
	h.resolveNewPaymentState()
	h.final(ctx)
	return nil
}
