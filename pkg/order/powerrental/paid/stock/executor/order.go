package executor

import (
	"context"
	"fmt"

	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	taskconfigmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/task/config"
	taskusermwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/task/user"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	apppowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	taskconfigmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/config"
	taskusermwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/user"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock/types"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent                      chan interface{}
	done                            chan interface{}
	appPowerRental                  *apppowerrentalmwpb.PowerRental
	existOrderCompletedHistory      bool
	existFirstOrderCompletedHistory bool
}

func (h *orderHandler) getAppPowerRental(ctx context.Context) error {
	good, err := apppowerrentalmwcli.GetPowerRental(ctx, h.AppGoodID)
	if err != nil {
		return err
	}
	if good == nil {
		return fmt.Errorf("invalid powerrental")
	}
	h.appPowerRental = good
	return nil
}

func (h *orderHandler) checkFirstOrderComplatedHistory(ctx context.Context) error {
	eventType := basetypes.UsedFor_FirstOrderCompleted
	ev, err := eventmwcli.GetEventOnly(ctx, &eventmwpb.Conds{
		AppID:     &basetypes.StringVal{Op: cruder.EQ, Value: h.appPowerRental.AppID},
		EventType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(eventType)},
	})
	if err != nil {
		return err
	}
	if ev == nil {
		return nil
	}
	taskConfig, err := taskconfigmwcli.GetTaskConfigOnly(ctx, &taskconfigmwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.appPowerRental.AppID},
		EventID: &basetypes.StringVal{Op: cruder.EQ, Value: ev.EntID},
	})
	if err != nil {
		return err
	}
	if taskConfig == nil {
		return nil
	}
	existTaskUser, err := taskusermwcli.ExistTaskUserConds(ctx, &taskusermwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.appPowerRental.AppID},
		EventID: &basetypes.StringVal{Op: cruder.EQ, Value: ev.EntID},
		TaskID:  &basetypes.StringVal{Op: cruder.EQ, Value: taskConfig.EntID},
		UserID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.UserID},
	})
	if err != nil {
		return err
	}
	if existTaskUser {
		h.existFirstOrderCompletedHistory = true
	}

	return nil
}

func (h *orderHandler) checkOrderComplatedHistory(ctx context.Context) error {
	eventType := basetypes.UsedFor_OrderCompleted
	ev, err := eventmwcli.GetEventOnly(ctx, &eventmwpb.Conds{
		AppID:     &basetypes.StringVal{Op: cruder.EQ, Value: h.appPowerRental.AppID},
		EventType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(eventType)},
	})
	if err != nil {
		return err
	}
	if ev == nil {
		return nil
	}
	taskConfig, err := taskconfigmwcli.GetTaskConfigOnly(ctx, &taskconfigmwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.appPowerRental.AppID},
		EventID: &basetypes.StringVal{Op: cruder.EQ, Value: ev.EntID},
	})
	if err != nil {
		return err
	}
	if taskConfig == nil {
		return nil
	}
	existTaskUser, err := taskusermwcli.ExistTaskUserConds(ctx, &taskusermwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.appPowerRental.AppID},
		EventID: &basetypes.StringVal{Op: cruder.EQ, Value: ev.EntID},
		TaskID:  &basetypes.StringVal{Op: cruder.EQ, Value: taskConfig.EntID},
		UserID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.UserID},
	})
	if err != nil {
		return err
	}
	if existTaskUser {
		h.existOrderCompletedHistory = true
	}

	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"AppPowerRental", h.appPowerRental,
			"Error", *err,
		)
	}
	existOrderCompletedHistory := false
	if h.existFirstOrderCompletedHistory || h.existOrderCompletedHistory {
		existOrderCompletedHistory = true
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder:           h.PowerRentalOrder,
		AppGoodStockLockID:         h.AppGoodStockLockID,
		ExistOrderCompletedHistory: existOrderCompletedHistory,
	}
	if h.appPowerRental != nil {
		persistentOrder.AppGoodStockID = h.appPowerRental.AppGoodStockID
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getAppPowerRental(ctx); err != nil {
		return err
	}

	h.existFirstOrderCompletedHistory = false
	h.existOrderCompletedHistory = false
	if err = h.checkFirstOrderComplatedHistory(ctx); err != nil {
		return err
	}
	if err = h.checkOrderComplatedHistory(ctx); err != nil {
		return err
	}

	return nil
}
