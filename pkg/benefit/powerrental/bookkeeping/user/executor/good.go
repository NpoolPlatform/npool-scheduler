package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	appfeemwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/fee"
	requiredappgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good/required"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appfeemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/fee"
	requiredappgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/required"
	apppowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/bookkeeping/user/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"

	"github.com/shopspring/decimal"
)

type coinReward struct {
	totalRewardAmount decimal.Decimal
	userRewardAmount  decimal.Decimal
}

type goodHandler struct {
	*powerrentalmwpb.PowerRental
	persistent         chan interface{}
	notif              chan interface{}
	done               chan interface{}
	totalOrderUnits    decimal.Decimal
	appOrderUnits      map[string]map[string]decimal.Decimal
	appPowerRentals    map[string]map[string]*apppowerrentalmwpb.PowerRental
	requiredAppFees    []*requiredappgoodmwpb.Required
	techniqueFees      map[string]*appfeemwpb.Fee
	appGoodUnitRewards map[string]map[string]map[string]decimal.Decimal
	orderRewards       []*types.OrderReward
	coinRewards        map[string]*coinReward
}

//nolint:dupl
func (h *goodHandler) getOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.appOrderUnits = map[string]map[string]decimal.Decimal{}

	for {
		orders, _, err := powerrentalordermwcli.GetPowerRentalOrders(ctx, &powerrentalordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
			BenefitState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitCalculated)},
			Simulate:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
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

func (h *goodHandler) getAppPowerRentals(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.appPowerRentals = map[string]map[string]*apppowerrentalmwpb.PowerRental{}

	for {
		goods, _, err := apppowerrentalmwcli.GetPowerRentals(ctx, &apppowerrentalmwpb.Conds{
			GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			_goods, ok := h.appPowerRentals[good.AppID]
			if !ok {
				_goods = map[string]*apppowerrentalmwpb.PowerRental{}
			}
			_goods[good.EntID] = good
			h.appPowerRentals[good.AppID] = _goods
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) calculateUnitRewardsLegacy() {
	for appID, appGoodUnits := range h.appOrderUnits {
		goods, ok := h.appPowerRentals[appID]
		if !ok {
			continue
		}
		appUnitRewards, ok := h.appGoodUnitRewards[appID]
		if !ok {
			appUnitRewards = map[string]map[string]decimal.Decimal{}
		}
		for appGoodID, units := range appGoodUnits {
			good, ok := goods[appGoodID]
			if !ok {
				continue
			}
			appGoodUnitRewards, ok := appUnitRewards[appGoodID]
			if !ok {
				appGoodUnitRewards = map[string]decimal.Decimal{}
			}
			for coinTypeID, reward := range h.coinRewards {
				if reward.userRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
					continue
				}
				userRewardAmount := reward.userRewardAmount.
					Mul(units).
					Div(h.totalOrderUnits)
				techniqueFee := userRewardAmount.
					Mul(decimal.RequireFromString(good.TechniqueFeeRatio)).
					Div(decimal.NewFromInt(100))
				appGoodUnitRewards[coinTypeID] = userRewardAmount.
					Sub(techniqueFee).
					Div(units)
			}
			appUnitRewards[appGoodID] = appGoodUnitRewards
		}
		h.appGoodUnitRewards[appID] = appUnitRewards
	}
}

//nolint:gocognit
func (h *goodHandler) _calculateUnitRewards() error {
	for appID, appGoodUnits := range h.appOrderUnits {
		// For one good, event it's assign to multiple app goods,
		// we'll use the same technique fee app good due to good only can bind to one technique fee good
		techniqueFee, ok := h.techniqueFees[appID]
		feePercent := decimal.NewFromInt(0)
		var err error

		if ok && techniqueFee.SettlementType == goodtypes.GoodSettlementType_GoodSettledByProfitPercent {
			feePercent, err = decimal.NewFromString(techniqueFee.UnitValue)
			if err != nil {
				return err
			}
		}

		appUnitRewards, ok := h.appGoodUnitRewards[appID]
		if !ok {
			appUnitRewards = map[string]map[string]decimal.Decimal{}
		}
		for appGoodID, units := range appGoodUnits {
			appGoodUnitRewards, ok := appUnitRewards[appGoodID]
			if !ok {
				appGoodUnitRewards = map[string]decimal.Decimal{}
			}
			for coinTypeID, reward := range h.coinRewards {
				if reward.userRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
					continue
				}
				userRewardAmount := reward.userRewardAmount.
					Mul(units).
					Div(h.totalOrderUnits)
				techniqueFee := userRewardAmount.
					Mul(feePercent).
					Div(decimal.NewFromInt(100))
				appGoodUnitRewards[coinTypeID] = userRewardAmount.
					Sub(techniqueFee).
					Div(units)
			}
			appUnitRewards[appGoodID] = appGoodUnitRewards
		}
		h.appGoodUnitRewards[appID] = appUnitRewards
	}
	return nil
}

func (h *goodHandler) getRequiredTechniqueFees(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		requireds, _, err := requiredappgoodmwcli.GetRequireds(ctx, &requiredappgoodmwpb.Conds{
			MainAppGoodIDs: &basetypes.StringSliceVal{
				Op: cruder.IN, Value: func() (appGoodIDs []string) {
					for _, appPowerRentals := range h.appPowerRentals {
						for _, appPowerRental := range appPowerRentals {
							appGoodIDs = append(appGoodIDs, appPowerRental.AppGoodID)
						}
					}
					return
				}(),
			},
			RequiredGoodType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(goodtypes.GoodType_TechniqueServiceFee)},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(requireds) == 0 {
			return nil
		}
		h.requiredAppFees = append(h.requiredAppFees, requireds...)
		offset += limit
	}
}

func (h *goodHandler) getAppTechniqueFees(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.techniqueFees = map[string]*appfeemwpb.Fee{}

	for {
		goods, _, err := appfeemwcli.GetFees(ctx, &appfeemwpb.Conds{
			AppGoodIDs: &basetypes.StringSliceVal{
				Op: cruder.IN, Value: func() (appGoodIDs []string) {
					for _, requiredAppFee := range h.requiredAppFees {
						appGoodIDs = append(appGoodIDs, requiredAppFee.RequiredAppGoodID)
					}
					return
				}(),
			},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			_, ok := h.techniqueFees[good.AppID]
			if ok {
				return fmt.Errorf("too many techniquefeegood")
			}
			h.techniqueFees[good.AppID] = good
		}
		offset += limit
	}

	return nil
}

func (h *goodHandler) constructCoinRewards() error {
	totalUnits, err := decimal.NewFromString(h.GoodTotal)
	if err != nil {
		return err
	}
	h.coinRewards = map[string]*coinReward{}
	for _, reward := range h.Rewards {
		totalRewardAmount, err := decimal.NewFromString(reward.LastRewardAmount)
		if err != nil {
			return err
		}
		userRewardAmount := totalRewardAmount.
			Mul(h.totalOrderUnits).
			Div(totalUnits)
		h.coinRewards[reward.CoinTypeID] = &coinReward{
			totalRewardAmount: totalRewardAmount,
			userRewardAmount:  userRewardAmount,
		}
	}
	return nil
}

func (h *goodHandler) calculateUnitRewards() error {
	h.appGoodUnitRewards = map[string]map[string]map[string]decimal.Decimal{}
	if h.totalOrderUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}
	if h.GoodType == goodtypes.GoodType_LegacyPowerRental {
		h.calculateUnitRewardsLegacy()
		return nil
	}
	return h._calculateUnitRewards()
}

func (h *goodHandler) calculateOrderReward(order *powerrentalordermwpb.PowerRentalOrder) error {
	appUnitRewards, ok := h.appGoodUnitRewards[order.AppID]
	if !ok {
		return nil
	}
	appGoodUnitRewards, ok := appUnitRewards[order.AppGoodID]
	if !ok {
		return nil
	}
	ioExtra := fmt.Sprintf(
		`{"GoodID":"%v","AppGoodID":"%v","OrderID":"%v","Units":"%v","BenefitDate":"%v"}`,
		h.GoodID,
		order.AppGoodID,
		order.OrderID,
		order.Units,
		h.LastRewardAt,
	)
	units, err := decimal.NewFromString(order.Units)
	if err != nil {
		return err
	}
	orderReward := &types.OrderReward{
		AppID:   order.AppID,
		UserID:  order.UserID,
		OrderID: order.OrderID,
		Extra:   ioExtra,
	}
	for coinTypeID := range h.coinRewards {
		unitReward, ok := appGoodUnitRewards[coinTypeID]
		if !ok {
			return nil
		}
		orderReward.CoinRewards = append(orderReward.CoinRewards, &types.CoinReward{
			CoinTypeID: coinTypeID,
			Amount:     unitReward.Mul(units).String(),
		})
	}
	h.orderRewards = append(h.orderRewards, orderReward)
	return nil
}

func (h *goodHandler) calculateOrderRewards(ctx context.Context) error {
	// If orderRewards is not empty, we do not update good benefit state, then we get next 20 orders
	orders, _, err := powerrentalordermwcli.GetPowerRentalOrders(ctx, &powerrentalordermwpb.Conds{
		GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		BenefitState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitCalculated)},
		Simulate:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
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
			"PowerRental", h.PowerRental,
			"OrderRewards", h.orderRewards,
			"AppOrderUnits", h.appOrderUnits,
			"Error", *err,
		)
	}
	persistentGood := &types.PersistentGood{
		PowerRental:  h.PowerRental,
		OrderRewards: h.orderRewards,
		Error:        *err,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}

	persistentGood.BenefitResult = basetypes.Result_Fail

	asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if err = h.getAppPowerRentals(ctx); err != nil {
		return err
	}
	if err := h.getRequiredTechniqueFees(ctx); err != nil {
		return err
	}
	if err := h.getAppTechniqueFees(ctx); err != nil {
		return err
	}
	if err = h.getOrderUnits(ctx); err != nil {
		return err
	}
	if err = h.constructCoinRewards(); err != nil {
		return err
	}
	if err := h.calculateUnitRewards(); err != nil {
		return err
	}
	if err = h.calculateOrderRewards(ctx); err != nil {
		return err
	}

	return nil
}
