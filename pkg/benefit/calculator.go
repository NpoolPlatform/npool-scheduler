package benefit

import (
	"context"
	"fmt"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	gbmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/goodbenefit"
	gbmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"

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

	return nil
}
