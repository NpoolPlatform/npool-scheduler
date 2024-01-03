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
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
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
	persistent              chan interface{}
	done                    chan interface{}
	notif                   chan interface{}
	requireds               []*requiredmwpb.Required
	appGoods                map[string]*appgoodmwpb.Good
	newRenewState           ordertypes.OrderRenewState
	childOrders             []*ordermwpb.Order
	techniqueFeeDuration    uint32
	electricityFeeDuration  uint32
	electricityFeeAppGoodID *string
	techniqueFeeAppGoodID   *string
	electricityFeeEndAt     uint32
	techniqueFeeEndAt       uint32
	userNotifText           string
	notifiable              bool
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

	if len(h.requireds) == 0 {
		return nil
	}

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
			h.appGoods[appGood.EntID] = appGood
		}
		offset += limit
	}

	return nil
}

func (h *orderHandler) renewGoodExist() (bool, error) {
	mainAppGood, ok := h.appGoods[h.AppGoodID]
	if !ok {
		return false, fmt.Errorf("invalid mainappgood")
	}
	if mainAppGood.PackageWithRequireds {
		return false, nil
	}
	needRenew := false
	for _, appGood := range h.appGoods {
		switch appGood.GoodType {
		case goodtypes.GoodType_ElectricityFee:
			h.electricityFeeAppGoodID = &appGood.EntID
			needRenew = true
		case goodtypes.GoodType_TechniqueServiceFee:
			if appGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByCash {
				h.techniqueFeeAppGoodID = &appGood.EntID
				needRenew = true
			}
		}
	}
	if !needRenew {
		return false, nil
	}

	return true, nil
}

func (h *orderHandler) getRenewableOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	appGoodIDs := []string{}
	if h.electricityFeeAppGoodID != nil {
		appGoodIDs = append(appGoodIDs, *h.electricityFeeAppGoodID)
	}
	if h.techniqueFeeAppGoodID != nil {
		appGoodIDs = append(appGoodIDs, *h.techniqueFeeAppGoodID)
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

	if h.electricityFeeAppGoodID != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == *h.electricityFeeAppGoodID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.electricityFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	if h.techniqueFeeAppGoodID != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == *h.techniqueFeeAppGoodID {
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

func (h *orderHandler) renewable() (bool, error) {
	now := uint32(time.Now().Unix())
	if h.EndAt <= now {
		return false, nil
	}

	orderElapsed := now - h.StartAt
	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour
	feeElapsed := orderElapsed - outOfGas - compensate

	if h.electricityFeeAppGoodID != nil {
		if h.electricityFeeDuration <= feeElapsed {
			// TODO: check user balance notify electricity fee renew
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*1 {
			// TODO: check user balance notify electricity fee renew
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*3 {
			// TODO: check user balance notify electricity fee renew
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*6 {
			// TODO: check user balance notify electricity fee renew
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*12 {
			// TODO: check user balance notify electricity fee renew
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*18 {
			// TODO: check user balance notify electricity fee renew
			// TODO: create electricity fee renew order
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*24 {
			// TODO: check user balance and notify electricity fee renew
		}
		h.electricityFeeEndAt = h.electricityFeeDuration + outOfGas + compensate
		h.notifiable = h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}
	if h.techniqueFeeAppGoodID != nil {
		if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerDay*2 {
			// TODO: check user balance and notify technique fee renew
		}
		if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerDay {
			// TODO: check user banalce and notify technique fee renew
			// TODO: create technique fee renew order
		}
		h.techniqueFeeEndAt = h.electricityFeeDuration + outOfGas + compensate
		h.notifiable = h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}

	return false, nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"NewRenewState", h.newRenewState,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:         h.Order,
		NewRenewState: h.newRenewState,
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, h.Order, h.notif)
	}
	if h.newRenewState != h.RenewState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.newRenewState = h.RenewState
	h.appGoods = map[string]*appgoodmwpb.Good{}

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
	if yes, err = h.renewable(); err != nil || !yes {
		return err
	}
	return nil
}
