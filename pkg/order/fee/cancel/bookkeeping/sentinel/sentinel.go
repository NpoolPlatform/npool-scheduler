package sentinel

import (
	"context"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/cancel/bookkeeping/types"
	feeordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/fee"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) scanFeeOrders(ctx context.Context, state ordertypes.OrderState, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := feeordermwcli.GetFeeOrders(ctx, &feeordermwpb.Conds{
			OrderState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}

		for _, order := range orders {
			cancelablefeed.CancelableFeed(ctx, order, exec)
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	return h.scanFeeOrders(ctx, ordertypes.OrderState_OrderStateCanceledTransferBookKeeping, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	if order, ok := ent.(*types.PersistentFeeOrder); ok {
		return order.UserID
	}
	return ent.(*feeordermwpb.FeeOrder).UserID
}
