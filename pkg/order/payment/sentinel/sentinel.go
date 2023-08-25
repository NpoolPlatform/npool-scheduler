package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/google/uuid"
)

type handler struct {
	basesentinel.Sentinel
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc, exec chan *ordermwpb.Order) {
	h = &handler{
		Sentinel: basesentinel.NewSentinel(ctx, cancel, h, time.Minute),
	}
	if err := h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStateCheckPayment); err != nil {
		logger.Sugar().Errorw(
			"run",
			"Error", err,
		)
	}
}

func (h *handler) feedOrder(ctx context.Context, order *ordermwpb.Order) error {
	if order.OrderState == ordertypes.OrderState_OrderStateWaitPayment {
		newState := ordertypes.OrderState_OrderStateCheckPayment
		if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
			ID:         &order.ID,
			OrderState: &newState,
		}); err != nil {
			return err
		}
	}
	h.Exec() <- order
	return nil
}

func (h *handler) scanOrderPayment(ctx context.Context, state ordertypes.OrderState) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			OrderState:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
			ParentOrderID: &basetypes.StringVal{Op: cruder.NEQ, Value: uuid.Nil.String()},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}

		for _, order := range orders {
			if err := h.feedOrder(ctx, order); err != nil {
				return err
			}
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context) error {
	return h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStateWaitPayment)
}
