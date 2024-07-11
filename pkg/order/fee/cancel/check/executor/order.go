package executor

import (
	"context"
	"fmt"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/cancel/check/types"
)

type orderHandler struct {
	*feeordermwpb.FeeOrder
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
	fmt.Printf("Cancel %v\n", h.OrderID)
	persistentFeeOrder := &types.PersistentFeeOrder{
		FeeOrder:        h.FeeOrder,
		NewPaymentState: h.newPaymentState,
	}
	asyncfeed.AsyncFeed(ctx, persistentFeeOrder, h.persistent)
}

func (h *orderHandler) exec(ctx context.Context) error {
	h.resolveNewPaymentState()
	h.final(ctx)
	return nil
}
