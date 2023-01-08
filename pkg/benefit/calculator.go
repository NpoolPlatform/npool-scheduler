package benefit

import (
	"context"
	"fmt"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	gbmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/goodbenefit"
	gbmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"

	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	paymentmgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/payment"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/appgood"
	appgoodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/appgood"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/appgood"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
)

func benefitTimestamp(timestamp uint32, interval time.Duration) uint32 {
	intervalFloat := interval.Seconds()
	intervalUint := uint32(intervalFloat)
	return timestamp / intervalUint * intervalUint
}

func (st *State) coin(ctx context.Context, coinTypeID string) (*coinmwpb.Coin, error) {
	coin, ok := st.Coins[coinTypeID]
	if ok {
		return coin, nil
	}

	coin, err := coinmwcli.GetCoin(ctx, coinTypeID)
	if err != nil {
		return nil, err
	}

	st.Coins[coinTypeID] = coin

	return coin, nil
}

func (st *State) goodBenefit(ctx context.Context, good *Good) (*gbmwpb.Account, error) {
	acc, ok := st.GoodBenefits[good.ID]
	if ok {
		return acc, nil
	}

	acc, err := gbmwcli.GetAccountOnly(ctx, &gbmwpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: good.ID,
		},
		Backup: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
	})
	if err != nil {
		return nil, err
	}
	if acc == nil {
		return nil, fmt.Errorf("invalid good benefit")
	}

	st.GoodBenefits[good.ID] = acc

	return acc, nil
}

func (st *State) balance(ctx context.Context, good *Good) (decimal.Decimal, error) {
	benefit, err := st.goodBenefit(ctx, good)
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	coin, err := st.coin(ctx, good.CoinTypeID)
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: benefit.Address,
	})
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	return decimal.NewFromString(balance.BalanceStr)
}

func (st *State) CalculateReward(ctx context.Context, good *Good) error {
	if good.GetGoodTotal() == 0 {
		return fmt.Errorf("invalid stock")
	}

	bal, err := st.balance(ctx, good)
	if err != nil {
		return err
	}

	if bal.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	coin, err := st.coin(ctx, good.CoinTypeID)
	if err != nil {
		return err
	}

	reservedAmount, err := decimal.NewFromString(coin.ReservedAmount)
	if err != nil {
		return err
	}

	good.TodayRewardAmount = bal.Sub(reservedAmount)
	good.UserRewardAmount = good.TodayRewardAmount.
		Mul(decimal.NewFromInt(int64(good.GoodInService))).
		Div(decimal.NewFromInt(int64(good.GoodTotal)))
	good.PlatformRewardAmount = good.TodayRewardAmount.
		Sub(good.UserRewardAmount)

	return nil
}

func validateOrder(good *goodmwpb.Good, order *ordermwpb.Order) bool {
	if order.PaymentState != paymentmgrpb.PaymentState_Done {
		return false
	}

	orderEnd := order.Start + uint32(good.DurationDays*timedef.SecondsPerDay)
	if orderEnd < uint32(time.Now().Unix()) {
		return false
	}
	if order.Start > uint32(time.Now().Unix()) {
		return false
	}
	if uint32(time.Now().Unix()) < order.Start+uint32(benefitInterval.Seconds()) {
		return false
	}

	return true
}

func (st *State) CalculateTechniqueServiceFee(ctx context.Context, good *Good) error {
	appUnits := map[string]uint32{}
	offset := int32(0)
	limit := int32(100)

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: good.ID,
			},
			State: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(ordermgrpb.OrderState_InService),
			},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}

		for _, ord := range orders {
			if !validateOrder(good.Good, ord) {
				continue
			}
			appUnits[ord.AppID] += ord.Units
			good.BenefitOrderIDs = append(good.BenefitOrderIDs, ord.ID)
		}

		offset += limit
	}

	appIDs := []string{}
	totalInService := uint32(0)

	for appID, units := range appUnits {
		appIDs = append(appIDs, appID)
		totalInService += units
	}

	if good.GoodInService != totalInService {
		return fmt.Errorf("inconsistent in service")
	}

	goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmgrpb.Conds{
		AppIDs: &commonpb.StringSliceVal{
			Op:    cruder.IN,
			Value: appIDs,
		},
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: good.ID,
		},
	}, int32(0), int32(len(appIDs)))
	if err != nil {
		return err
	}

	goodMap := map[string]*appgoodmwpb.Good{}
	for _, g := range goods {
		goodMap[g.AppID] = g
	}

	techniqueServiceFee := decimal.NewFromInt(0)

	for appID, units := range appUnits {
		ag, ok := goodMap[appID]
		if !ok {
			return fmt.Errorf("unauthorized appgood")
		}

		_fee := good.UserRewardAmount.
			Mul(decimal.NewFromInt(int64(units))).
			Div(decimal.NewFromInt(int64(totalInService))).
			Mul(decimal.NewFromInt(int64(ag.TechnicalFeeRatio))).
			Div(decimal.NewFromInt(100))

		logger.Sugar().Infow("CalculateTechniqueServiceFee",
			"GoodID", good.ID,
			"GoodName", good.Title,
			"TotalInService", totalInService,
			"AppID", appID,
			"Units", units,
			"TechnicalFeeRatio", ag.TechnicalFeeRatio,
			"FeeAmount", _fee,
		)

		techniqueServiceFee = techniqueServiceFee.Add(_fee)
	}

	good.TechniqueServiceFeeAmount = techniqueServiceFee
	good.UserRewardAmount.Sub(techniqueServiceFee)

	return nil
}
