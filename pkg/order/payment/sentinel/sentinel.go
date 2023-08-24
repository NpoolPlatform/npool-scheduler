package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"

	"github.com/google/uuid"
)

type handler struct {
	w    *watcher.Watcher
	exec chan *ordermwpb.Order
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc, exec chan *ordermwpb.Order) {
	go action.Watch(ctx, cancel, func(_ctx context.Context) {
		h = &handler{
			w:    watcher.NewWatcher(),
			exec: exec,
		}
		h.run(_ctx)
	})
}

func (h *handler) feedOrder(ctx context.Context, order *ordermwpb.Order) error {
	if order.OrderState == ordertypes.OrderState_OrderStateWaitPayment {
		newState := ordertypes.OrderState_OrderStateCheckPayment
		if _, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
			ID:    &order.ID,
			State: &newState,
		}); err != nil {
			return err
		}
	}
	h.exec <- order
	return nil
}

func (h *handler) scanOrderPayment(ctx context.Context, state ordertypes.OrderState) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			State:         &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
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

func (h *handler) handler(ctx context.Context) bool {
	const scanInterval = 30 * time.Second
	ticker := time.NewTicker(scanInterval)

	select {
	case <-ticker.C:
		if err := h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStateWaitPayment); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "scanWaitPayment",
				"Error", err,
			)
		}
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"handler",
			"State", "Done",
			"Error", ctx.Err(),
		)
		close(h.w.ClosedChan())
		return true
	case <-h.w.CloseChan():
		close(h.w.ClosedChan())
		return true
	}
}

func (h *handler) run(ctx context.Context) {
	// Feed the interrupted check payment
	if err := h.scanOrderPayment(ctx, ordertypes.OrderState_OrderStateCheckPayment); err != nil {
		logger.Sugar().Errorw(
			"run",
			"Error", err,
		)
		return
	}
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func Finalize() {
	if h != nil && h.w != nil {
		h.w.Shutdown()
	}
}
