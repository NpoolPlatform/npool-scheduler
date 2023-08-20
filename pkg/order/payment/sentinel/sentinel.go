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

	// basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	commonpb "github.com/NpoolPlatform/message/npool"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
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

func (h *handler) scanWaitPayment(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			// State: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(types.OrderState_WaitPayment)},
			State: &commonpb.Uint32Val{Op: cruder.EQ, Value: uint32(10)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}

		for _, order := range orders {
			h.exec <- order
		}

		offset += limit
	}
}

func (h *handler) handler(ctx context.Context) bool {
	const scanInterval = 30 * time.Second
	ticker := time.NewTicker(scanInterval)

	select {
	case <-ticker.C:
		if err := h.scanWaitPayment(ctx); err != nil {
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
