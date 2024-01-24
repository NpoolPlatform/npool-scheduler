package executor

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	renewcommon "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/wait/types"
)

type orderHandler struct {
	*renewcommon.OrderHandler
	persistent chan interface{}
	done       chan interface{}
	notif      chan interface{}
	notifiable bool
}

func (h *orderHandler) checkNotifiable() bool {
	now := uint32(time.Now().Unix())
	if h.StartAt >= now || h.EndAt <= now {
		return false
	}
	if h.MainAppGood.PackageWithRequireds {
		return false
	}
	h.notifiable = h.CheckElectricityFee || h.CheckTechniqueFee
	return h.notifiable
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"notifiable", h.notifiable,
			"CheckTechniqueFee", h.CheckTechniqueFee,
			"CheckElectricityFee", h.CheckElectricityFee,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order: h.Order,
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, h.Order, h.notif)
	}
	if h.notifiable {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	var yes bool
	defer h.final(ctx, &err)

	if err = h.GetRequireds(ctx); err != nil {
		return err
	}
	if err := h.GetAppGoods(ctx); err != nil {
		return err
	}
	if yes, err = h.RenewGoodExist(); err != nil || !yes {
		return err
	}
	if err = h.GetRenewableOrders(ctx); err != nil {
		return err
	}
	if yes = h.checkNotifiable(); !yes {
		return nil
	}
	return nil
}
