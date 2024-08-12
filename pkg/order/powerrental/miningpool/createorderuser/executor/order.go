package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	powerrentalgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	goodusermwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/gooduser"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/createorderuser/types"
	"github.com/google/uuid"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	appPowerRental *powerrentalgoodmwpb.PowerRental

	powerRentalOrderReq *powerrentalordermwpb.PowerRentalOrderReq
	orderUserReq        *orderusermwpb.OrderUserReq
	poolGoodUserID      *string
	persistent          chan interface{}
	done                chan interface{}
	notif               chan interface{}
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

func (h *orderHandler) checkAppPowerRental() error {
	if h.appPowerRental == nil {
		return fmt.Errorf("invalid powerrental")
	}
	if h.appPowerRental.State != goodtypes.GoodState_GoodStateReady {
		return fmt.Errorf("powerrental good not ready")
	}
	return nil
}

func (h *orderHandler) getPoolGoodUserID() error {
	if h.appPowerRental == nil {
		return fmt.Errorf("invalid powerrental")
	}

	var miningGoodStockID *string
	var poolGoodOrderID *string
	for _, appMiningGoodStock := range h.appPowerRental.AppMiningGoodStocks {
		if appMiningGoodStock.EntID == h.PowerRentalOrder.AppGoodStockID {
			miningGoodStockID = &appMiningGoodStock.MiningGoodStockID
			break
		}
	}

	if miningGoodStockID == nil {
		return fmt.Errorf("cannot find appmininggoodstock, appgoodstockid: %v", h.PowerRentalOrder.AppGoodStockID)
	}

	for _, miningGoodStock := range h.appPowerRental.MiningGoodStocks {
		if miningGoodStock.EntID == *miningGoodStockID {
			poolGoodOrderID = &miningGoodStock.PoolGoodUserID
			break
		}
	}

	if poolGoodOrderID == nil {
		return fmt.Errorf("cannot find mininggoodstock, mininggoodstockid: %v", miningGoodStockID)
	}

	h.poolGoodUserID = poolGoodOrderID
	return nil
}

func (h *orderHandler) validatePoolGoodUserID(ctx context.Context) error {
	if h.poolGoodUserID == nil {
		return fmt.Errorf("invalid poolgooduserid")
	}
	info, err := goodusermwcli.GetGoodUser(ctx, *h.poolGoodUserID)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("invalid poolgooduserid")
	}
	return nil
}

func (h *orderHandler) constructCreateOrderUserReq() {
	h.orderUserReq = &orderusermwpb.OrderUserReq{
		EntID:      func() *string { entID := uuid.NewString(); return &entID }(),
		AppID:      &h.PowerRentalOrder.AppID,
		UserID:     &h.PowerRentalOrder.UserID,
		GoodUserID: h.poolGoodUserID,
	}
}

func (h *orderHandler) constructUpdatePowerrentalOrderReq() {
	h.powerRentalOrderReq = &powerrentalordermwpb.PowerRentalOrderReq{
		ID:              &h.PowerRentalOrder.ID,
		EntID:           &h.PowerRentalOrder.EntID,
		PoolOrderUserID: h.orderUserReq.EntID,
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
		PowerRentalOrderReq: h.powerRentalOrderReq,
		OrderUserReq:        h.orderUserReq,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
	}

	asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getAppPowerRental(ctx); err != nil {
		return err
	}

	if err = h.checkAppPowerRental(); err != nil {
		return err
	}

	if err = h.validatePoolGoodUserID(ctx); err != nil {
		return err
	}

	if err = h.getPoolGoodUserID(); err != nil {
		return err
	}

	h.constructCreateOrderUserReq()
	h.constructUpdatePowerrentalOrderReq()

	return nil
}
