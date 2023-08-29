package executor

import (
	"context"
	"fmt"
	"time"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/start/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent chan interface{}
}

func (h *orderHandler) startable() (bool, error) {
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
	if uint32(time.Now().Unix()) < h.StartAt {
		return false, nil
	}
	return true, nil
}

func (h *orderHandler) final() {
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	h.persistent <- persistentOrder
}

func (h *orderHandler) exec(ctx context.Context) error {
	if yes, err := h.startable(); err != nil || yes {
		return err
	}
	h.final()
	return nil
}