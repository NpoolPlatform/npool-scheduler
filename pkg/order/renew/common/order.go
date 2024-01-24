package executor

import (
	"context"
	"fmt"
	"sort"

	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
)

type OrderHandler struct {
	*ordermwpb.Order
	requireds              []*requiredmwpb.Required
	MainAppGood            *appgoodmwpb.Good
	ElectricityFeeAppGood  *appgoodmwpb.Good
	TechniqueFeeAppGood    *appgoodmwpb.Good
	childOrders            []*ordermwpb.Order
	TechniqueFeeDuration   uint32
	ElectricityFeeDuration uint32
}

func (h *OrderHandler) GetRequireds(ctx context.Context) error {
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

func (h *OrderHandler) GetAppGoods(ctx context.Context) error {
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
				h.ElectricityFeeAppGood = appGood
			case goodtypes.GoodType_TechniqueServiceFee:
				h.TechniqueFeeAppGood = appGood
			}
			if appGood.EntID == h.AppGoodID {
				h.MainAppGood = appGood
			}
		}
		offset += limit
	}

	if h.MainAppGood == nil {
		return fmt.Errorf("invalid mainappgood")
	}

	return nil
}

func (h *OrderHandler) RenewGoodExist() (bool, error) {
	if h.MainAppGood.PackageWithRequireds {
		return false, nil
	}
	return h.TechniqueFeeAppGood != nil || h.ElectricityFeeAppGood != nil, nil
}

func (h *OrderHandler) GetRenewableOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	appGoodIDs := []string{}
	if h.ElectricityFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.ElectricityFeeAppGood.EntID)
	}
	if h.TechniqueFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.TechniqueFeeAppGood.EntID)
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

	if h.ElectricityFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.ElectricityFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.ElectricityFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	if h.TechniqueFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.TechniqueFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.TechniqueFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	return nil
}
