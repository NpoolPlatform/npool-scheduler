package executor

import (
	"context"

	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/created/types"
)

type orderHandler struct {
	*feeordermwpb.FeeOrder
	persistent chan interface{}
}

func (h *orderHandler) final(ctx context.Context) {
	persistentFeeOrder := &types.PersistentFeeOrder{
		FeeOrder: h.FeeOrder,
	}
	asyncfeed.AsyncFeed(ctx, persistentFeeOrder, h.persistent)
}

func (h *orderHandler) exec(ctx context.Context) error {
	h.final(ctx)
	return nil
}
