package accounting

import (
	"context"
	"time"

	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"

	billingpb "github.com/NpoolPlatform/cloud-hashing-billing/message/npool"
	goodspb "github.com/NpoolPlatform/cloud-hashing-goods/message/npool"
	orderpb "github.com/NpoolPlatform/cloud-hashing-order/message/npool"
	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"

	goodsconst "github.com/NpoolPlatform/cloud-hashing-goods/pkg/const"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
)

type goodAccounting struct {
	good                  *goodspb.GoodInfo
	coininfo              *coininfopb.CoinInfo
	goodsetting           *billingpb.PlatformSetting
	accounts              map[string]*billingpb.CoinAccountInfo
	benefits              []*billingpb.PlatformBenefit
	transactions          []*billingpb.CoinAccountTransaction
	preQueryBalance       float64
	afterQueryBalanceInfo *sphinxproxypb.BalanceInfo
	orders                []*orderpb.Order
	compensates           map[string][]*orderpb.Compensate
	userUnits             uint32
	platformUnits         uint32
}

type accounting struct {
	ticker          *time.Ticker
	goodAccountings []*goodAccounting
}

func (ac *accounting) onQueryGoods(ctx context.Context) {
	resp, err := grpc2.GetGoods(ctx, &goodspb.GetGoodsRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get goods: %v", err)
		return
	}

	acs := []*goodAccounting{}
	for _, good := range resp.Infos {
		acs = append(acs, &goodAccounting{
			good:        good,
			accounts:    map[string]*billingpb.CoinAccountInfo{},
			compensates: map[string][]*orderpb.Compensate{},
		})
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryCoininfo(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
			ID: gac.good.CoinInfoID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get coin info: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.coininfo = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryAccount(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetPlatformSettingByGood(ctx, &billingpb.GetPlatformSettingByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get platform setting by good: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.goodsetting = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryAccountInfo(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.BenefitAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good benefit account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.BenefitAccountID] = resp.Info

		resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.PlatformOfflineAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good platform offline account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.PlatformOfflineAccountID] = resp.Info

		resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.UserOnlineAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good user online benefit account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.UserOnlineAccountID] = resp.Info

		resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.UserOfflineAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good user offline benefit account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.UserOfflineAccountID] = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryBenefits(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetPlatformBenefitsByGood(ctx, &billingpb.GetPlatformBenefitsByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get platform benefits by good: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.benefits = resp.Infos
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQuerySpendTransactions(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetCoinAccountTransactionsByCoinAccount(ctx, &billingpb.GetCoinAccountTransactionsByCoinAccountRequest{
			CoinTypeID: gac.good.CoinInfoID,
			AddressID:  gac.goodsetting.BenefitAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get benefit account transaction by good: %v [%v]", err, gac.good.ID)
			continue
		}

		for _, info := range resp.Infos {
			if info.ToAddressID == gac.goodsetting.BenefitAccountID {
				logger.Sugar().Errorf("good benefit account should not accept platform incoming transaction: %v [%v]", info.ToAddressID, gac.good.ID)
				continue
			}
		}

		gac.transactions = resp.Infos
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryBalance(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		inComing := float64(0)
		outComing := float64(0)

		for _, benefit := range gac.benefits {
			inComing += benefit.Amount
		}

		for _, spend := range gac.transactions {
			outComing += spend.Amount
		}

		if inComing < outComing {
			logger.Sugar().Errorf("address %v invalid incoming %v < outcoming %v [%v]", gac.goodsetting.BenefitAccountID, inComing, outComing, gac.good.ID)
			continue
		}

		resp, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    gac.coininfo.Name,
			Address: gac.accounts[gac.goodsetting.BenefitAccountID].Address,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get balance for good benefit account %v: %v [%v]", gac.goodsetting.BenefitAccountID, err, gac.good.ID)
			continue
		}

		gac.preQueryBalance = inComing - outComing
		gac.afterQueryBalanceInfo = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryOrders(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetOrdersByGood(ctx, &orderpb.GetOrdersByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get orders by good: %v", err)
			continue
		}

		gac.orders = resp.Infos
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryCompensates(ctx context.Context) {
	for _, gac := range ac.goodAccountings {
		for _, order := range gac.orders {
			resp, err := grpc2.GetCompensatesByOrder(ctx, &orderpb.GetCompensatesByOrderRequest{
				OrderID: order.ID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail get compensates by order: %v", err)
				continue
			}

			gac.compensates[order.ID] = resp.Infos
		}
	}
}

func (ac *accounting) onCaculateUserBenefit() {
	for _, gac := range ac.goodAccountings {
		if gac.good.BenefitType == goodsconst.BenefitTypePool {
			continue
		}

		gac.userUnits = 0
		gac.platformUnits = 0
		goodDurationSeconds := uint32(gac.good.DurationDays * 24 * 60 * 60)
		nowSeconds := uint32(time.Now().Unix())

		for _, order := range gac.orders {
			compensateSeconds := uint32(0)
			for _, compensate := range gac.compensates[order.ID] {
				compensateSeconds += compensate.End - compensate.Start
			}

			if order.Start+goodDurationSeconds+compensateSeconds < nowSeconds {
				continue
			}

			gac.userUnits += order.Units
		}

		gac.platformUnits = uint32(gac.good.Total) - gac.userUnits
	}
}

func (ac *accounting) onPersistentResult(ctx context.Context) {
}

func Run(ctx context.Context) {
	ac := &accounting{
		ticker: time.NewTicker(30 * time.Second),
	}

	for {
		ac.onQueryGoods(ctx)
		ac.onQueryCoininfo(ctx)
		ac.onQueryAccount(ctx)
		ac.onQueryAccountInfo(ctx)
		ac.onQueryBenefits(ctx)
		ac.onQuerySpendTransactions(ctx)
		ac.onQueryBalance(ctx)
		ac.onQueryOrders(ctx)
		ac.onQueryCompensates(ctx)
		ac.onCaculateUserBenefit()
		ac.onPersistentResult(ctx)

		<-ac.ticker.C
	}
}
