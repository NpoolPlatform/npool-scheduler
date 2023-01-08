package benefit

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	gbmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/goodbenefit"
	gbmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"

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

func (st *State) balance(ctx context.Context, good *Good) (decimal.Decimal, error) {
	benefit, err := gbmwcli.GetAccountOnly(ctx, &gbmwpb.Conds{
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
		return decimal.NewFromInt(0), err
	}
	if benefit == nil {
		return decimal.NewFromInt(0), fmt.Errorf("invalid good benefit")
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
			appUnits[ord.AppID] += ord.Units
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
