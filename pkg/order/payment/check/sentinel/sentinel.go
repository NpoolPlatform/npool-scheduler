package sentinel

import (
	"context"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/check/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) feedOrder(ctx context.Context, order *ordermwpb.Order, exec chan interface{}) error {
	if order.OrderState == ordertypes.OrderState_OrderStateWaitPayment {
		newState := ordertypes.OrderState_OrderStateCheckPayment
		if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
			ID:         &order.ID,
			OrderState: &newState,
		}); err != nil {
			return err
		}
	}
	cancelablefeed.CancelableFeed(ctx, order, exec)
	return nil
}

func (h *handler) scanOrderPayment(ctx context.Context, state ordertypes.OrderState, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		// If order is PayWithParentOrder, they will done or canceled with parent order
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			OrderState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
			PaymentTypes: &basetypes.Uint32SliceVal{Op: cruder.IN, Value: []uint32{
				uint32(ordertypes.PaymentType_PayWithBalanceOnly),
				uint32(ordertypes.PaymentType_PayWithTransferOnly),
				uint32(ordertypes.PaymentType_PayWithTransferAndBalance),
				uint32(ordertypes.PaymentType_PayWithOffline),
				uint32(ordertypes.PaymentType_PayWithNoPayment),
			}},
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
	return h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStateWaitPayment, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStateCheckPayment, exec)
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
