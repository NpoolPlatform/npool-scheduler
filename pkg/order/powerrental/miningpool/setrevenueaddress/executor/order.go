package executor

import (
	"context"

	orderbenefitmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/orderbenefit"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	"github.com/NpoolPlatform/message/npool/account/mw/v1/orderbenefit"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	v1 "github.com/NpoolPlatform/message/npool/basetypes/v1"
	powerrentalgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderusermwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/orderuser"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/setrevenueaddress/types"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder

	appPowerRental       *powerrentalgoodmwpb.PowerRental
	orderbenefitAccounts map[string]*orderbenefit.Account
	powerRentalOrderReq  *powerrentalordermwpb.PowerRentalOrderReq
	nextState            ordertypes.OrderState

	coinTypeIDs   []string
	orderUserReqs []*orderusermwpb.OrderUserReq
	persistent    chan interface{}
	done          chan interface{}
	notif         chan interface{}
}

func (h *orderHandler) getAppPowerRental(ctx context.Context) error {
	good, err := apppowerrentalmwcli.GetPowerRental(ctx, h.AppGoodID)
	if err != nil {
		return wlog.WrapError(err)
	}
	if good == nil {
		return wlog.Errorf("invalid powerrental")
	}
	h.appPowerRental = good
	return nil
}

func (h *orderHandler) getOrderBenefits(ctx context.Context) error {
	accounts, _, err := orderbenefitmwcli.GetAccounts(ctx, &orderbenefit.Conds{
		OrderID: &v1.StringVal{
			Op:    cruder.EQ,
			Value: h.PowerRentalOrder.OrderID,
		},
	}, 0, 0)
	if err != nil {
		return wlog.WrapError(err)
	}

	h.orderbenefitAccounts = make(map[string]*orderbenefit.Account)
	for _, acc := range accounts {
		h.orderbenefitAccounts[acc.CoinTypeID] = acc
	}
	return nil
}

func (h *orderHandler) checkAppPowerRental() error {
	if h.appPowerRental == nil {
		return wlog.Errorf("invalid powerrental")
	}
	if h.appPowerRental.State != goodtypes.GoodState_GoodStateReady {
		return wlog.Errorf("powerrental good not ready")
	}
	return nil
}

func (h *orderHandler) getCoinTypeIDs() error {
	for _, goodCoin := range h.appPowerRental.GoodCoins {
		h.coinTypeIDs = append(h.coinTypeIDs, goodCoin.CoinTypeID)
	}

	if len(h.coinTypeIDs) == 0 {
		return wlog.Errorf("have no goodcoins")
	}
	return nil
}

func (h *orderHandler) validatePoolOrderUserID(ctx context.Context) error {
	if h.PowerRentalOrder.PoolOrderUserID == nil {
		return wlog.Errorf("invalid poolorderuserid")
	}

	info, err := orderusermwcli.GetOrderUser(ctx, *h.PowerRentalOrder.PoolOrderUserID)
	if err != nil {
		return wlog.WrapError(err)
	}
	if info == nil {
		return wlog.Errorf("invalid poolorderuserid")
	}
	return nil
}

func (h *orderHandler) constructOrderUserReqs() error {
	h.orderUserReqs = []*orderusermwpb.OrderUserReq{}
	autoPay := true
	for _, coinTypeID := range h.coinTypeIDs {
		acc, ok := h.orderbenefitAccounts[coinTypeID]
		if !ok {
			return wlog.Errorf("cannot find orderbenefit account for cointypeid: %v", coinTypeID)
		}
		h.orderUserReqs = append(h.orderUserReqs, &orderusermwpb.OrderUserReq{
			EntID:          h.PowerRentalOrder.PoolOrderUserID,
			RevenueAddress: &acc.Address,
			CoinTypeID:     &coinTypeID,
			AutoPay:        &autoPay,
		})
	}
	return nil
}

func (h *orderHandler) constructPowerRentalOrderReq() {
	h.powerRentalOrderReq = &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &h.PowerRentalOrder.ID,
		OrderState: &h.nextState,
	}
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"AdminSetCanceled", h.AdminSetCanceled,
			"UserSetCanceled", h.UserSetCanceled,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder:    h.PowerRentalOrder,
		OrderUserReqs:       h.orderUserReqs,
		PowerRentalOrderReq: h.powerRentalOrderReq,
		AppGoodStockLockID:  &h.PowerRentalOrder.AppGoodStockLockID,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
	} else {
		asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.done)
	}
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.nextState = ordertypes.OrderState_OrderStateInService

	var err error
	defer h.final(ctx, &err)

	if h.PowerRentalOrder.GoodStockMode != goodtypes.GoodStockMode_GoodStockByMiningPool {
		h.constructPowerRentalOrderReq()
		return nil
	}

	if err = h.getAppPowerRental(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.checkAppPowerRental(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.getCoinTypeIDs(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.getOrderBenefits(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.validatePoolOrderUserID(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.constructOrderUserReqs(); err != nil {
		return wlog.WrapError(err)
	}

	h.constructPowerRentalOrderReq()

	return nil
}
