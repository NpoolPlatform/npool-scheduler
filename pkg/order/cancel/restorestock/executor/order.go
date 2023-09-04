package executor

import (
	"context"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/restorestock/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent chan interface{}
}

func (h *orderHandler) final(ctx context.Context) {
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	cancelablefeed.CancelableFeed(ctx, persistentOrder, h.persistent)
}

func (h *orderHandler) exec(ctx context.Context) error { //nolint
	h.final(ctx)
	return nil
}
