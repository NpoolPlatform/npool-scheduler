package executor

import (
	"context"

	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/cancel/bookkeeping/types"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent chan interface{}
	done       chan interface{}
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	persistentPowerRentalOrder := &types.PersistentPowerRentalOrder{
		PowerRentalOrder: h.PowerRentalOrder,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentPowerRentalOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentPowerRentalOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)
	return nil
}
