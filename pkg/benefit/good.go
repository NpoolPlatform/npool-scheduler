package benefit

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	_ "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	_ "github.com/NpoolPlatform/message/npool/cloud-hashing-goods"

	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	_ "github.com/NpoolPlatform/message/npool/coininfo"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	profitdetailpb "github.com/NpoolPlatform/message/npool/miningmgr/profit/detail"
	profitgeneralpb "github.com/NpoolPlatform/message/npool/miningmgr/profit/general"
	profitdetailcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/detail"
	profitgeneralcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/general"

	stockcli "github.com/NpoolPlatform/stock-manager/pkg/client"
	stockconst "github.com/NpoolPlatform/stock-manager/pkg/const"

	commonpb "github.com/NpoolPlatform/message/npool"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	"github.com/shopspring/decimal"
)

// TODO: support multiple coin type profit of one good

type gp struct {
	goodID          string
	goodName        string
	profitGeneralID string

	totalUnits      uint32
	inService       uint32
	totalOrderUnits uint32

	benefitAddress       string
	benefitAccountID     string
	benefitIntervalHours uint32
	dailyProfit          decimal.Decimal

	userOnlineAddress      string
	platformOfflineAddress string

	coinName   string
	coinTypeID string
}

func (g *gp) profitExist(ctx context.Context, timestamp time.Time) (bool, error) {
	detail, err := profitdetailcli.GetDetailOnly(ctx, &profitdetailpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.goodID,
		},
		BenefitDate: &commonpb.Uint32Val{
			Op:    cruder.EQ,
			Value: uint32(timestamp.Unix()),
		},
	})
	if err != nil {
		return false, err
	}

	return detail != nil, nil
}

func (g *gp) profitBalance(ctx context.Context) (decimal.Decimal, error) {
	general, err := profitgeneralcli.GetGeneralOnly(ctx, &profitgeneralpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.goodID,
		},
	})
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if general == nil {
		_, err = profitgeneralcli.CreateGeneral(ctx, &profitgeneralpb.GeneralReq{
			GoodID:     &g.goodID,
			CoinTypeID: &g.coinTypeID,
		})
		if err != nil {
			return decimal.NewFromInt(0), err
		}

		g.profitGeneralID = general.ID

		return decimal.NewFromInt(0), nil
	}

	g.profitGeneralID = general.ID

	amount, err := decimal.NewFromString(general.Amount)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	toPlatform, err := decimal.NewFromString(general.ToPlatform)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	toUser, err := decimal.NewFromString(general.ToUser)
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	remain := amount.Sub(toPlatform).Sub(toUser)
	if remain.Cmp(decimal.NewFromInt(0)) < 0 {
		return decimal.NewFromInt(0), fmt.Errorf("invalid profit general")
	}

	return remain, nil
}

func (g *gp) addDailyProfit(ctx context.Context, timestamp time.Time) error {
	if g.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid profit amount")
	}

	if g.totalUnits <= 0 {
		return fmt.Errorf("invalid stock units")
	}

	amount := g.dailyProfit.String()
	toUserD := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(g.totalOrderUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits)))
	toUser := toUserD.String()
	toPlatform := g.dailyProfit.Sub(toUserD).String()

	tsUnix := uint32(timestamp.Unix())

	_, err := profitdetailcli.CreateDetail(ctx, &profitdetailpb.DetailReq{
		GoodID:      &g.goodID,
		CoinTypeID:  &g.coinTypeID,
		Amount:      &amount,
		BenefitDate: &tsUnix,
	})
	if err != nil {
		return err
	}

	_, err = profitgeneralcli.AddGeneral(ctx, &profitgeneralpb.GeneralReq{
		ID:         &g.profitGeneralID,
		Amount:     &amount,
		ToPlatform: &toPlatform,
		ToUser:     &toUser,
	})

	return err
}

func (g *gp) benefitBalance(ctx context.Context) (decimal.Decimal, error) {
	benefit, err := billingcli.GetGoodBenefit(ctx, g.goodID)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if benefit == nil {
		return decimal.NewFromInt(0), fmt.Errorf("invalid good benefit setting")
	}

	g.benefitAccountID = benefit.BenefitAccountID
	g.benefitIntervalHours = benefit.BenefitIntervalHours

	account, err := billingcli.GetAccount(ctx, g.benefitAccountID)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if account == nil {
		return decimal.NewFromInt(0), fmt.Errorf("invalid benefit account")
	}

	g.benefitAddress = account.Address

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    g.coinName,
		Address: g.benefitAddress,
	})
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	return decimal.NewFromString(balance.BalanceStr)
}

func (g *gp) processDailyProfit(ctx context.Context, timestamp time.Time) error {
	exist, err := g.profitExist(ctx, timestamp)
	if err != nil {
		return err
	}
	if exist {
		return fmt.Errorf("daily profit exist")
	}

	remain, err := g.profitBalance(ctx)
	if err != nil {
		return err
	}

	balance, err := g.benefitBalance(ctx)
	if err != nil {
		return err
	}

	logger.Sugar().Infow("processDailyProfit", "goodID", g.goodID, "goodName", g.goodName, "benefitAddress",
		g.benefitAddress, "remain", remain, "balance", balance)

	// TODO: if too less, do not transfer
	if balance.Cmp(remain) <= 0 {
		return nil
	}

	g.dailyProfit = balance.Sub(remain)

	return nil
}

func (g *gp) stock(ctx context.Context) error {
	stock, err := stockcli.GetStockOnly(ctx, cruder.NewFilterConds().
		WithCond(stockconst.StockFieldGoodID, cruder.EQ, structpb.NewStringValue(g.goodID)))
	if err != nil {
		return err
	}
	if stock == nil {
		return fmt.Errorf("invalid good stock")
	}

	g.totalUnits = stock.Total
	g.inService = stock.InService

	return nil
}

func (g *gp) processOrder(ctx context.Context, order *orderpb.Order, timestamp time.Time) error {
	logger.Sugar().Infow("processOrder", "timestamp", timestamp, "goodID", g.goodID, "goodName", g.goodName, "profit",
		g.dailyProfit, "totalUnits", g.totalUnits, "order", order.ID, "orderUnits", order.Units)
	return nil
}
