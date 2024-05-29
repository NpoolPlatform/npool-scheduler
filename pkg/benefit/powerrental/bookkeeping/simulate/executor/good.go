package executor

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	simprofitmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/simulate/ledger/profit"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	simprofitmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/simulate/ledger/profit"
	orderappconfigmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/app/config"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/simulate/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	orderappconfigmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/app/config"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent                 chan interface{}
	notif                      chan interface{}
	done                       chan interface{}
	unitRewardAmount           decimal.Decimal
	appSimulateOrderUnits      map[string]map[string]decimal.Decimal
	appGoods                   map[string]map[string]*appgoodmwpb.Good
	goodCreatedAt              uint32
	appGoodUnitSimulateRewards map[string]map[string]decimal.Decimal
	orderRewards               []*types.OrderReward
	appSimulateConfig          map[string]*orderappconfigmwpb.SimulateConfig
}

//nolint:dupl
func (h *goodHandler) getSimulateOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	simulate := true

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
			BenefitState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitCalculated)},
			Simulate:      &basetypes.BoolVal{Op: cruder.EQ, Value: simulate},
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
			appGoodUnits, ok := h.appSimulateOrderUnits[order.AppID]
			if !ok {
				appGoodUnits = map[string]decimal.Decimal{
					order.AppGoodID: decimal.NewFromInt(0),
				}
			}
			appGoodUnits[order.AppGoodID] = appGoodUnits[order.AppGoodID].Add(units)
			h.appSimulateOrderUnits[order.AppID] = appGoodUnits
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

func (h *goodHandler) getAppSimulateConfig(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		configs, _, err := orderappconfigmwcli.GetSimulateConfigs(ctx, &orderappconfigmwpb.Conds{}, offset, limit)
		if err != nil {
			return err
		}
		if len(configs) == 0 {
			break
		}
		for _, config := range configs {
			h.appSimulateConfig[config.AppID] = config
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) checkFirstProfit(ctx context.Context, order *ordermwpb.Order) bool {
	profit, err := simprofitmwcli.GetProfitOnly(ctx, &simprofitmwpb.Conds{
		AppID:  &basetypes.StringVal{Op: cruder.EQ, Value: order.AppID},
		UserID: &basetypes.StringVal{Op: cruder.EQ, Value: order.UserID},
	})
	if err != nil {
		logger.Sugar().Errorw(
			"checkFirstProfit",
			"AppID", order.AppID,
			"UserID", order.UserID,
			"Error", err,
		)
		return false
	}
	if profit != nil {
		return false
	}
	return true
}

func (h *goodHandler) calculateCashable(config *orderappconfigmwpb.SimulateConfig) bool {
	probability, err := decimal.NewFromString(config.CashableProfitProbability)
	if err != nil {
		logger.Sugar().Errorw(
			"calculateCashable",
			"AppID", config.AppID,
			"CashableProfitProbability", config.CashableProfitProbability,
			"Error", err,
		)
		return false
	}
	if probability.Cmp(decimal.NewFromInt(0)) <= 0 {
		return false
	}
	if probability.Cmp(decimal.NewFromInt(1)) >= 0 {
		return true
	}

	rand.Seed(time.Now().UnixNano())
	value := rand.Float64() //nolint
	return decimal.NewFromFloat(value).Cmp(probability) <= 0
}

func (h *goodHandler) sendCouponable(ctx context.Context, config *orderappconfigmwpb.SimulateConfig, order *ordermwpb.Order) bool {
	switch config.SendCouponMode {
	case ordertypes.SendCouponMode_WithoutCoupon:
		return false
	case ordertypes.SendCouponMode_FirstBenifit:
		return h.checkFirstProfit(ctx, order)
	case ordertypes.SendCouponMode_FirstAndRandomBenifit:
		firstProfit := h.checkFirstProfit(ctx, order)
		if firstProfit {
			return true
		}
	case ordertypes.SendCouponMode_RandomBenifit:
	default:
		err := fmt.Errorf("invalid sendcouponmode")
		logger.Sugar().Errorw(
			"sendCouponable",
			"AppID", config.AppID,
			"SendCouponMode", config.SendCouponMode,
			"Error", err,
		)
		return false
	}

	probability, err := decimal.NewFromString(config.SendCouponProbability)
	if err != nil {
		logger.Sugar().Errorw(
			"sendCouponable",
			"AppID", config.AppID,
			"SendCouponProbability", config.SendCouponProbability,
			"Error", err,
		)
		return false
	}
	if probability.Cmp(decimal.NewFromInt(0)) <= 0 {
		return false
	}
	if probability.Cmp(decimal.NewFromInt(1)) >= 0 {
		return true
	}

	rand.Seed(time.Now().UnixNano())
	value := rand.Float64() //nolint
	return decimal.NewFromFloat(value).Cmp(probability) <= 0
}

func (h *goodHandler) calculateSimulateOrderReward(ctx context.Context, order *ordermwpb.Order) error {
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
	amount := h.unitRewardAmount.Mul(units)
	sendCoupon := false
	cashable := false
	simulateConfig, ok := h.appSimulateConfig[order.AppID]
	if ok {
		sendCoupon = h.sendCouponable(ctx, simulateConfig, order)
		cashable = h.calculateCashable(simulateConfig)
	}
	h.orderRewards = append(h.orderRewards, &types.OrderReward{
		AppID:      order.AppID,
		UserID:     order.UserID,
		OrderID:    order.ID,
		Amount:     amount.String(),
		Extra:      ioExtra,
		SendCoupon: sendCoupon,
		Cashable:   cashable,
	})
	return nil
}

func (h *goodHandler) calculateOrderRewards(ctx context.Context) error {
	// If orderRewards is not empty, we do not update good benefit state, then we get get next 20 orders
	simulate := true
	orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
		GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		BenefitState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitCalculated)},
		Simulate:      &basetypes.BoolVal{Op: cruder.EQ, Value: simulate},
	}, 0, int32(20))
	if err != nil {
		return err
	}
	if len(orders) == 0 {
		return nil
	}

	for _, order := range orders {
		if err := h.calculateSimulateOrderReward(ctx, order); err != nil {
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
			"unitRewardAmount", h.unitRewardAmount,
			"OrderRewards", h.orderRewards,
			"AppSimulateOrderUnits", h.appSimulateOrderUnits,
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
	h.appSimulateOrderUnits = map[string]map[string]decimal.Decimal{}
	h.appGoodUnitSimulateRewards = map[string]map[string]decimal.Decimal{}
	h.appSimulateConfig = map[string]*orderappconfigmwpb.SimulateConfig{}
	var err error

	defer h.final(ctx, &err)

	h.unitRewardAmount, err = decimal.NewFromString(h.LastUnitRewardAmount)
	if err != nil {
		return err
	}
	if err = h.getAppGoods(ctx); err != nil {
		return err
	}

	if err = h.getSimulateOrderUnits(ctx); err != nil {
		return err
	}

	if err := h.getAppSimulateConfig(ctx); err != nil {
		return err
	}

	if err = h.calculateOrderRewards(ctx); err != nil {
		return err
	}

	return nil
}
