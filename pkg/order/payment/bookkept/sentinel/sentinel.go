package sentinel

import (
	"context"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/bookkept/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) feedOrder(ctx context.Context, order *ordermwpb.Order, exec chan interface{}) error {
	if order.OrderState == ordertypes.OrderState_OrderStatePaymentTransferBookKept {
		newState := ordertypes.OrderState_OrderStatePaymentTransferBookKeptCheck
		if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
			ID:         &order.ID,
			OrderState: &newState,
		}); err != nil {
			return err
		}
	}
	exec <- order
	return nil
}

func (h *handler) scanOrderPayment(ctx context.Context, state ordertypes.OrderState, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			OrderState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}

		for _, order := range orders {
			if err := h.feedOrder(ctx, order, exec); err != nil {
				return err
			}
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	return h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStatePaymentTransferBookKept, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStatePaymentTransferBookKeptCheck, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	if order, ok := ent.(*types.PersistentOrder); ok {
		return order.ID
	}
	return ent.(*ordermwpb.Order).ID
}
