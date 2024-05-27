package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/restorestock/types"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent chan interface{}
	notif      chan interface{}
	done       chan interface{}
	appGood    *appgoodmwpb.Good
}

func (h *orderHandler) getAppGood(ctx context.Context) error {
	good, err := appgoodmwcli.GetGood(ctx, h.AppGoodID)
	if err != nil {
		return err
	}
	if good == nil {
		return fmt.Errorf("invalid good")
	}
	h.appGood = good
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"AppGood", h.appGood,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:              h.Order,
		AppGoodStockLockID: h.AppGoodStockLockID,
	}
	if h.appGood != nil {
		persistentOrder.AppGoodStockID = h.appGood.AppGoodStockID
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getAppGood(ctx); err != nil {
		return err
	}

	return nil
}
