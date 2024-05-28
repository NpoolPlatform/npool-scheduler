package executor

import (
	"context"

	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/created/types"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent chan interface{}
}

func (h *orderHandler) final(ctx context.Context) {
	persistentPowerRentalOrder := &types.PersistentPowerRentalOrder{
		PowerRentalOrder: h.PowerRentalOrder,
	}
	asyncfeed.AsyncFeed(ctx, persistentPowerRentalOrder, h.persistent)
}

func (h *orderHandler) exec(ctx context.Context) error {
	h.final(ctx)
	return nil
}
