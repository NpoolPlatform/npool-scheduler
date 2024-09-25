package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	taskconfigmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/task/config"
	taskusermwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/task/user"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	taskconfigmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/config"
	taskusermwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/user"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/unlockaccount/types"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent                      chan interface{}
	notif                           chan interface{}
	done                            chan interface{}
	paymentAccounts                 map[string]*paymentaccountmwpb.Account
	existOrderCompletedHistory      bool
	existFirstOrderCompletedHistory bool
}

func (h *orderHandler) payWithTransfer() bool {
	return len(h.PaymentTransfers) > 0
}

func (h *orderHandler) checkUnlockable() bool {
	return h.payWithTransfer()
}

func (h *orderHandler) getPaymentAccounts(ctx context.Context) (err error) {
	h.paymentAccounts, err = schedcommon.GetPaymentAccounts(ctx, func() (accountIDs []string) {
		for _, paymentTransfer := range h.PaymentTransfers {
			accountIDs = append(accountIDs, paymentTransfer.AccountID)
		}
		return
	}())
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, paymentTransfer := range h.PaymentTransfers {
		if _, ok := h.paymentAccounts[paymentTransfer.AccountID]; !ok {
			return wlog.Errorf("invalid paymentaccount")
		}
	}
	return nil
}

func (h *orderHandler) checkFirstOrderComplatedHistory(ctx context.Context) error {
	eventType := basetypes.UsedFor_FirstOrderCompleted
	ev, err := eventmwcli.GetEventOnly(ctx, &eventmwpb.Conds{
		AppID:     &basetypes.StringVal{Op: cruder.EQ, Value: h.PowerRentalOrder.AppID},
		EventType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(eventType)},
	})
	if err != nil {
		return err
	}
	if ev == nil {
		return nil
	}
	taskConfig, err := taskconfigmwcli.GetTaskConfigOnly(ctx, &taskconfigmwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.PowerRentalOrder.AppID},
		EventID: &basetypes.StringVal{Op: cruder.EQ, Value: ev.EntID},
	})
	if err != nil {
		return err
	}
	if taskConfig == nil {
		return nil
	}
	existTaskUser, err := taskusermwcli.ExistTaskUserConds(ctx, &taskusermwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.PowerRentalOrder.AppID},
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
		AppID:     &basetypes.StringVal{Op: cruder.EQ, Value: h.PowerRentalOrder.AppID},
		EventType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(eventType)},
	})
	if err != nil {
		return err
	}
	if ev == nil {
		return nil
	}
	taskConfig, err := taskconfigmwcli.GetTaskConfigOnly(ctx, &taskconfigmwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.PowerRentalOrder.AppID},
		EventID: &basetypes.StringVal{Op: cruder.EQ, Value: ev.EntID},
	})
	if err != nil {
		return err
	}
	if taskConfig == nil {
		return nil
	}
	existTaskUser, err := taskusermwcli.ExistTaskUserConds(ctx, &taskusermwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.PowerRentalOrder.AppID},
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
			"PaymentAccounts", h.paymentAccounts,
			"Error", *err,
		)
	}
	existOrderCompletedHistory := false
	if h.existFirstOrderCompletedHistory || h.existOrderCompletedHistory {
		existOrderCompletedHistory = true
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder: h.PowerRentalOrder,
		PaymentAccountIDs: func() (ids []uint32) {
			for _, paymentAccount := range h.paymentAccounts {
				ids = append(ids, paymentAccount.ID)
			}
			return
		}(),
		ExistOrderCompletedHistory: existOrderCompletedHistory,
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

	if able := h.checkUnlockable(); !able {
		return nil
	}
	if err = h.getPaymentAccounts(ctx); err != nil {
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
