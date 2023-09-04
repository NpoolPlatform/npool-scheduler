package executor

import (
	"context"
	"fmt"
	"time"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/check/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent chan interface{}
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
	default:
		return false, fmt.Errorf("invalid paymentstate")
	}
	if h.EndAt >= uint32(time.Now().Unix()) {
		return false, nil
	}
	return true, nil
}

func (h *orderHandler) final() {
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	asyncfeed.AsyncFeed(persistentOrder, h.persistent)
}

func (h *orderHandler) exec(ctx context.Context) error { //nolint
	if yes, err := h.expired(); err != nil || !yes {
		return err
	}
	h.final()
	return nil
}
