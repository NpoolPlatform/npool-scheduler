package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/user/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent           chan interface{}
	notif                chan interface{}
	done                 chan interface{}
	totalRewardAmount    decimal.Decimal
	totalUnits           decimal.Decimal
	totalOrderUnits      decimal.Decimal
	appOrderUnits        map[string]map[string]decimal.Decimal
	appGoods             map[string]map[string]*appgoodmwpb.Good
	goodCreatedAt        uint32
	techniqueFeeAppGoods map[string]*appgoodmwpb.Good
	userRewardAmount     decimal.Decimal
	appGoodUnitRewards   map[string]map[string]decimal.Decimal
	orderRewards         []*types.OrderReward
}

func (h *goodHandler) getOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
			BenefitState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitCalculated)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}
		for _, order := range orders {
			units, err := decimal.NewFromString(order.Units)
			if err != nil {
				return err
			}
			h.totalOrderUnits = h.totalOrderUnits.Add(units)
			appGoodUnits, ok := h.appOrderUnits[order.AppID]
			if !ok {
				appGoodUnits = map[string]decimal.Decimal{
					order.AppGoodID: decimal.NewFromInt(0),
				}
			}
			appGoodUnits[order.AppGoodID] = appGoodUnits[order.AppGoodID].Add(units)
			h.appOrderUnits[order.AppID] = appGoodUnits
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) getAppGoods(ctx context.Context) error {
	good, err := goodmwcli.GetGood(ctx, h.EntID)
	if err != nil {
		return err
	}

	h.goodCreatedAt = good.CreatedAt

	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			_goods, ok := h.appGoods[good.AppID]
			if !ok {
				_goods = map[string]*appgoodmwpb.Good{}
			}
			_goods[good.EntID] = good
			h.appGoods[good.AppID] = _goods
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) calculateUnitRewardLegacy() {
	for appID, appGoodUnits := range h.appOrderUnits {
		goods, ok := h.appGoods[appID]
		if !ok {
			continue
		}
		unitRewards, ok := h.appGoodUnitRewards[appID]
		if !ok {
			unitRewards = map[string]decimal.Decimal{}
		}
		for appGoodID, units := range appGoodUnits {
			good, ok := goods[appGoodID]
			if !ok {
				continue
			}
			userRewardAmount := h.userRewardAmount.
				Mul(units).
				Div(h.totalOrderUnits)
			techniqueFee := userRewardAmount.
				Mul(decimal.RequireFromString(good.TechnicalFeeRatio)).
				Div(decimal.NewFromInt(100))
			unitRewards[appGoodID] = userRewardAmount.
				Sub(techniqueFee).
				Div(units)
		}
		h.appGoodUnitRewards[appID] = unitRewards
	}
}

func (h *goodHandler) _calculateUnitReward() error {
	for appID, appGoodUnits := range h.appOrderUnits {
		// For one good, event it's assign to multiple app goods,
		// we'll use the same technique fee app good due to good only can bind to one technique fee good
		techniqueFeeAppGood, ok := h.techniqueFeeAppGoods[appID]
		feePercent := decimal.NewFromInt(0)
		var err error

		if ok && techniqueFeeAppGood.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfit {
			feePercent, err = decimal.NewFromString(techniqueFeeAppGood.UnitPrice)
			if err != nil {
				return err
			}
		}

		unitRewards, ok := h.appGoodUnitRewards[appID]
		if !ok {
			unitRewards = map[string]decimal.Decimal{}
		}
		for appGoodID, units := range appGoodUnits {
			userRewardAmount := h.userRewardAmount.
				Mul(units).
				Div(h.totalOrderUnits)
			techniqueFee := userRewardAmount.
				Mul(feePercent).
				Div(decimal.NewFromInt(100))
			unitRewards[appGoodID] = userRewardAmount.
				Sub(techniqueFee).
				Div(units)
		}
		h.appGoodUnitRewards[appID] = unitRewards
	}
	return nil
}

func (h *goodHandler) getTechniqueFeeGoods(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	requireds := []*requiredmwpb.Required{}

	for {
		_requireds, _, err := requiredmwcli.GetRequireds(ctx, &requiredmwpb.Conds{
			MainGoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(_requireds) == 0 {
			break
		}
		requireds = append(requireds, _requireds...)
		offset += limit
	}

	offset = 0
	requiredGoodIDs := []string{}
	for _, required := range requireds {
		requiredGoodIDs = append(requiredGoodIDs, required.RequiredGoodID)
	}

	for {
		goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			GoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: requiredGoodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			if good.GoodType != goodtypes.GoodType_TechniqueServiceFee {
				continue
			}
			_, ok := h.techniqueFeeAppGoods[good.AppID]
			if !ok {
				return fmt.Errorf("too many techniquefeegood")
			}
			h.techniqueFeeAppGoods[good.AppID] = good
		}
		offset += limit
	}

	return nil
}

func (h *goodHandler) calculateUnitReward(ctx context.Context) error {
	if h.totalOrderUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}
	if h.userRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	const legacyTechniqueFeeTimestamp = 1704009402
	if h.goodCreatedAt <= legacyTechniqueFeeTimestamp {
		h.calculateUnitRewardLegacy()
		return nil
	}

	if err := h.getTechniqueFeeGoods(ctx); err != nil {
		return err
	}

	return h._calculateUnitReward()
}

func (h *goodHandler) calculateOrderReward(order *ordermwpb.Order) error {
	unitRewards, ok := h.appGoodUnitRewards[order.AppID]
	if !ok {
		return nil
	}
	unitReward, ok := unitRewards[order.AppGoodID]
	if !ok {
		return nil
	}
	ioExtra := fmt.Sprintf(
		`{"GoodID":"%v","AppGoodID":"%v","OrderID":"%v","Units":"%v","BenefitDate":"%v"}`,
		h.EntID,
		order.AppGoodID,
		order.EntID,
		order.Units,
		h.LastRewardAt,
	)
	units, err := decimal.NewFromString(order.Units)
	if err != nil {
		return err
	}
	amount := unitReward.Mul(units)
	h.orderRewards = append(h.orderRewards, &types.OrderReward{
		AppID:   order.AppID,
		UserID:  order.UserID,
		OrderID: order.ID,
		Amount:  amount.String(),
		Extra:   ioExtra,
	})
	return nil
}

func (h *goodHandler) calculateOrderRewards(ctx context.Context) error {
	// If orderRewards is not empty, we do not update good benefit state, then we get get next 20 orders
	orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
		GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		BenefitState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitCalculated)},
	}, 0, int32(20))
	if err != nil {
		return err
	}
	if len(orders) == 0 {
		return nil
	}

	for _, order := range orders {
		if err := h.calculateOrderReward(order); err != nil {
			return err
		}
	}
	return nil
}

//nolint:gocritic
func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Good", h.Good,
			"TotalRewardAmount", h.totalRewardAmount,
			"UserRewardAmount", h.userRewardAmount,
			"OrderRewards", h.orderRewards,
			"AppOrderUnits", h.appOrderUnits,
			"Error", *err,
		)
	}
	persistentGood := &types.PersistentGood{
		Good:         h.Good,
		OrderRewards: h.orderRewards,
		Error:        *err,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}

	persistentGood.BenefitResult = basetypes.Result_Fail
	persistentGood.BenefitMessage = (*err).Error()

	asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	h.appGoods = map[string]map[string]*appgoodmwpb.Good{}
	h.techniqueFeeAppGoods = map[string]*appgoodmwpb.Good{}
	h.appOrderUnits = map[string]map[string]decimal.Decimal{}
	h.appGoodUnitRewards = map[string]map[string]decimal.Decimal{}
	var err error

	defer h.final(ctx, &err)

	h.totalRewardAmount, err = decimal.NewFromString(h.LastRewardAmount)
	if err != nil {
		return err
	}
	if err = h.getAppGoods(ctx); err != nil {
		return err
	}
	h.totalUnits, err = decimal.NewFromString(h.GoodTotal)
	if err != nil {
		return err
	}
	if err = h.getOrderUnits(ctx); err != nil {
		return err
	}
	h.userRewardAmount = h.totalRewardAmount.
		Mul(h.totalOrderUnits).
		Div(h.totalUnits)
	if err := h.calculateUnitReward(ctx); err != nil {
		return err
	}
	if err = h.calculateOrderRewards(ctx); err != nil {
		return err
	}

	return nil
}
