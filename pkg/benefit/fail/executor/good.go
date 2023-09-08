package executor

import (
	"context"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/fail/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent      chan interface{}
	done            chan interface{}
	benefitOrderIDs []string
}

func (h *goodHandler) getBenefitOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}
		for _, order := range orders {
			h.benefitOrderIDs = append(h.benefitOrderIDs, order.ID)
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) final(ctx context.Context, err *error) {
	persistentGood := &types.PersistentGood{
		Good:            h.Good,
		BenefitOrderIDs: h.benefitOrderIDs,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

func (h *goodHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getBenefitOrders(ctx); err != nil {
		return err
	}

	return nil
}
