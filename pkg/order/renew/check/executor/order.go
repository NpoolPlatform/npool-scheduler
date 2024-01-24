package executor

import (
	"context"
	"fmt"
	"sort"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/check/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent             chan interface{}
	done                   chan interface{}
	notif                  chan interface{}
	requireds              []*requiredmwpb.Required
	mainAppGood            *appgoodmwpb.Good
	electricityFeeAppGood  *appgoodmwpb.Good
	techniqueFeeAppGood    *appgoodmwpb.Good
	childOrders            []*ordermwpb.Order
	techniqueFeeDuration   uint32
	electricityFeeDuration uint32
	notifyElectricityFee   bool
	notifyTechniqueFee     bool
	notifiable             bool
}

func (h *orderHandler) getRequireds(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		requireds, _, err := requiredmwcli.GetRequireds(ctx, &requiredmwpb.Conds{
			MainGoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(requireds) == 0 {
			break
		}
		h.requireds = append(h.requireds, requireds...)
		offset += limit
	}
	return nil
}

func (h *orderHandler) getAppGoods(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	goodIDs := []string{h.GoodID}
	for _, required := range h.requireds {
		goodIDs = append(goodIDs, required.RequiredGoodID)
	}

	for {
		appGoods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
			GoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: goodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(appGoods) == 0 {
			break
		}
		for _, appGood := range appGoods {
			switch appGood.GoodType {
			case goodtypes.GoodType_ElectricityFee:
				h.electricityFeeAppGood = appGood
			case goodtypes.GoodType_TechniqueServiceFee:
				h.techniqueFeeAppGood = appGood
			}
			if appGood.EntID == h.AppGoodID {
				h.mainAppGood = appGood
			}
		}
		offset += limit
	}

	if h.mainAppGood == nil {
		return fmt.Errorf("invalid mainappgood")
	}

	return nil
}

func (h *orderHandler) renewGoodExist() (bool, error) {
	if h.mainAppGood.PackageWithRequireds {
		return false, nil
	}
	return h.techniqueFeeAppGood != nil || h.electricityFeeAppGood != nil, nil
}

func (h *orderHandler) getRenewableOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	appGoodIDs := []string{}
	if h.electricityFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.electricityFeeAppGood.EntID)
	}
	if h.techniqueFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.techniqueFeeAppGood.EntID)
	}

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			ParentOrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			AppGoodIDs:    &basetypes.StringSliceVal{Op: cruder.IN, Value: appGoodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}
		h.childOrders = append(h.childOrders, orders...)
		offset += limit
	}

	sort.Slice(h.childOrders, func(i, j int) bool {
		return h.childOrders[i].StartAt < h.childOrders[j].StartAt
	})

	if h.electricityFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.electricityFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.electricityFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	if h.techniqueFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.techniqueFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.techniqueFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	return nil
}

func (h *orderHandler) checkNotifiable() bool {
	now := uint32(time.Now().Unix())
	if h.StartAt >= now || h.EndAt <= now {
		return false
	}

	orderElapsed := now - h.StartAt
	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour

	ignoredSeconds := outOfGas + compensate
	feeElapsed := orderElapsed - ignoredSeconds

	if h.electricityFeeAppGood != nil {
		h.notifyElectricityFee = h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}
	if h.techniqueFeeAppGood != nil && h.techniqueFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByCash {
		h.notifyTechniqueFee = h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}

	h.notifiable = h.notifyElectricityFee || h.notifyTechniqueFee
	return h.notifiable
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"notifiable", h.notifiable,
			"notifyTechniqueFee", h.notifyTechniqueFee,
			"notifyElectricityFee", h.notifyElectricityFee,
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

	if err = h.getRequireds(ctx); err != nil {
		return err
	}
	if err := h.getAppGoods(ctx); err != nil {
		return err
	}
	if yes, err = h.renewGoodExist(); err != nil || !yes {
		return err
	}
	if err = h.getRenewableOrders(ctx); err != nil {
		return err
	}
	if yes = h.checkNotifiable(); !yes {
		return nil
	}
	return nil
}
