package executor

import (
	"context"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/expiry/preexpired/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent chan interface{}
}

func (h *orderHandler) final() {
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	h.persistent <- persistentOrder
}

func (h *orderHandler) exec(ctx context.Context) error {
	h.final()
	return nil
}
