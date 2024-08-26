package executor

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	simprofitmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/simulate/ledger/profit"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	apppowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	simprofitmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/simulate/ledger/profit"
	orderappconfigmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/app/config"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/bookkeeping/simulate/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	orderappconfigmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/app/config"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*powerrentalmwpb.PowerRental
	persistent                 chan interface{}
	notif                      chan interface{}
	done                       chan interface{}
	appPowerRentals            map[string]map[string]*apppowerrentalmwpb.PowerRental
	appGoodUnitSimulateRewards map[string]map[string]decimal.Decimal
	orderRewards               []*types.OrderReward
	appOrderConfigs            map[string]*orderappconfigmwpb.AppConfig
}

func (h *goodHandler) getAppPowerRentals(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.appPowerRentals = map[string]map[string]*apppowerrentalmwpb.PowerRental{}

	for {
		goods, _, err := apppowerrentalmwcli.GetPowerRentals(ctx, &apppowerrentalmwpb.Conds{
			GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
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

func (h *goodHandler) getAppOrderConfig(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.appOrderConfigs = map[string]*orderappconfigmwpb.AppConfig{}

	for {
		configs, _, err := orderappconfigmwcli.GetAppConfigs(ctx, &orderappconfigmwpb.Conds{}, offset, limit)
		if err != nil {
			return err
		}
		if len(configs) == 0 {
			break
		}
		for _, config := range configs {
			h.appOrderConfigs[config.AppID] = config
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) checkFirstProfit(ctx context.Context, order *powerrentalordermwpb.PowerRentalOrder) bool {
	profit, err := simprofitmwcli.GetProfitOnly(ctx, &simprofitmwpb.Conds{
		AppID:  &basetypes.StringVal{Op: cruder.EQ, Value: order.AppID},
		UserID: &basetypes.StringVal{Op: cruder.EQ, Value: order.UserID},
	})
	if err != nil {
		return false
	}
	if profit != nil {
		return false
	}
	return true
}

func (h *goodHandler) cashable(config *orderappconfigmwpb.AppConfig) bool {
	probability, err := decimal.NewFromString(config.SimulateOrderCashableProfitProbability)
	if err != nil {
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

func (h *goodHandler) shouldSendCoupon(ctx context.Context, config *orderappconfigmwpb.AppConfig, order *powerrentalordermwpb.PowerRentalOrder) bool {
	switch config.SimulateOrderCouponMode {
	case ordertypes.SimulateOrderCouponMode_WithoutCoupon:
		return false
	case ordertypes.SimulateOrderCouponMode_FirstBenifit:
		return h.checkFirstProfit(ctx, order)
	case ordertypes.SimulateOrderCouponMode_FirstAndRandomBenifit:
		firstProfit := h.checkFirstProfit(ctx, order)
		if firstProfit {
			return true
		}
	case ordertypes.SimulateOrderCouponMode_RandomBenifit:
	default:
		return false
	}

	probability, err := decimal.NewFromString(config.SimulateOrderCouponProbability)
	if err != nil {
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

func (h *goodHandler) calculateSimulateOrderReward(ctx context.Context, order *powerrentalordermwpb.PowerRentalOrder) error {
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
	sendCoupon := false
	cashable := false
	simulateConfig, ok := h.appOrderConfigs[order.AppID]
	if ok {
		sendCoupon = h.shouldSendCoupon(ctx, simulateConfig, order)
		cashable = h.cashable(simulateConfig)
	}
	orderReward := &types.OrderReward{
		AppID:   order.AppID,
		UserID:  order.UserID,
		OrderID: order.OrderID,
		Extra:   ioExtra,
	}
	for _, reward := range h.Rewards {
		unitRewardAmount, err := decimal.NewFromString(reward.LastUnitRewardAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		amount := unitRewardAmount.Mul(units)
		if amount.LessThanOrEqual(decimal.NewFromInt(0)) {
			continue
		}
		orderReward.CoinRewards = append(orderReward.CoinRewards, &types.CoinReward{
			CoinTypeID: reward.CoinTypeID,
			Amount:     amount.String(),
			Cashable:   reward.MainCoin && cashable,
			SendCoupon: reward.MainCoin && sendCoupon,
		})
	}
	h.orderRewards = append(h.orderRewards, orderReward)
	return nil
}

func (h *goodHandler) calculateOrderRewards(ctx context.Context) error {
	// If orderRewards is not empty, we do not update good benefit state, then we get next 20 orders
	orders, _, err := powerrentalordermwcli.GetPowerRentalOrders(ctx, &powerrentalordermwpb.Conds{
		GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
		LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		BenefitState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitCalculated)},
		Simulate:      &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		CreatedAt:     &basetypes.Uint32Val{Op: cruder.LT, Value: uint32(time.Now().Unix() - timedef.SecondsPerDay)},
		StartAt:       &basetypes.Uint32Val{Op: cruder.LT, Value: uint32(time.Now().Unix() - timedef.SecondsPerDay)},
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
			"PowerRental", h.PowerRental,
			"OrderRewards", h.orderRewards,
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
	persistentGood.BenefitMessage = wlog.Unwrap(*err).Error()

	asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	h.appGoodUnitSimulateRewards = map[string]map[string]decimal.Decimal{}
	var err error

	defer h.final(ctx, &err)

	if err = h.getAppPowerRentals(ctx); err != nil {
		return err
	}
	if err = h.getAppOrderConfig(ctx); err != nil {
		return err
	}
	if err = h.calculateOrderRewards(ctx); err != nil {
		return err
	}

	return nil
}
