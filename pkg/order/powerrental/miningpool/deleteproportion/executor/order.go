package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderusermwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/orderuser"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/deleteproportion/types"
	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	appPowerRental      *powerrentalgoodmwpb.PowerRental
	orderUserReqs       []*orderusermwpb.OrderUserReq
	powerRentalOrderReq *powerrentalordermwpb.PowerRentalOrderReq

	coinTypeIDs []string
	proportion  string
	nextState   ordertypes.OrderState
	persistent  chan interface{}
	done        chan interface{}
	notif       chan interface{}
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

func (h *orderHandler) getProportion() error {
	zeroDec, err := decimal.NewFromString("0")
	if err != nil {
		return wlog.WrapError(err)
	}

	h.proportion = zeroDec.String()
	return nil
}

func (h *orderHandler) constructOrderUserReqs() {
	for _, coinTypeID := range h.coinTypeIDs {
		h.orderUserReqs = append(h.orderUserReqs, &orderusermwpb.OrderUserReq{
			EntID:      h.PoolOrderUserID,
			CoinTypeID: &coinTypeID,
			Proportion: &h.proportion,
		})
	}
}

func (h *orderHandler) constructPowerRentalOrderReq() {
	h.powerRentalOrderReq = &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &h.ID,
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
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
	} else {
		asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.done)
	}
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.nextState = ordertypes.OrderState_OrderStateExpired

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
	if err = h.getProportion(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.validatePoolOrderUserID(ctx); err != nil {
		return wlog.WrapError(err)
	}

	h.constructOrderUserReqs()
	h.constructPowerRentalOrderReq()

	return nil
}
