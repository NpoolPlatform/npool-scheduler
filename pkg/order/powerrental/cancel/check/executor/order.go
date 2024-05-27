package executor

import (
	"context"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/check/types"
)

type powerRentalOrderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent      chan interface{}
	newPaymentState *ordertypes.PaymentState
}

func (h *powerRentalOrderHandler) resolveNewPaymentState() {
	if h.OrderState == ordertypes.OrderState_OrderStateWaitPayment {
		state := ordertypes.PaymentState_PaymentStateCanceled
		h.newPaymentState = &state
	}
}

func (h *powerRentalOrderHandler) final(ctx context.Context) {
	persistentPowerRentalOrder := &types.PersistentPowerRentalOrder{
		PowerRentalOrder: h.PowerRentalOrder,
		NewPaymentState:  h.newPaymentState,
	}
	asyncfeed.AsyncFeed(ctx, persistentPowerRentalOrder, h.persistent)
}

func (h *powerRentalOrderHandler) exec(ctx context.Context) error {
	h.resolveNewPaymentState()
	h.final(ctx)
	return nil
}
