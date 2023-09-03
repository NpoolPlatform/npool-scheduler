package executor

import (
	"context"
	"fmt"

	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/spent/types"
)

type orderHandler struct {
	*ordermwpb.Order
	appGood    *appgoodmwpb.Good
	persistent chan interface{}
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
func (h *orderHandler) final(err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"AppGood", h.appGood,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:          h.Order,
		AppGoodStockID: h.appGood.AppGoodStockID,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(persistentOrder, h.persistent)
	}
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(&err)

	if err = h.getAppGood(ctx); err != nil {
		return err
	}

	return nil
}
