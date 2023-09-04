package executor

import (
	"context"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/restorestock/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent chan interface{}
}

func (h *orderHandler) final() {
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	asyncfeed.AsyncFeed(persistentOrder, h.persistent)
}

func (h *orderHandler) exec(ctx context.Context) error { //nolint
	h.final()
	return nil
}
