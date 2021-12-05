package accounting

import (
	"context"
	"time"

	billingpb "github.com/NpoolPlatform/cloud-hashing-billing/message/npool"
	goodspb "github.com/NpoolPlatform/cloud-hashing-goods/message/npool"
	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
)

type goodAccounting struct {
	good         *goodspb.GoodInfo
	goodsetting  *billingpb.PlatformSetting
	benefits     []*billingpb.PlatformBenefit
	transactions []*billingpb.CoinAccountTransaction
}

type accounting struct {
	ticker                 *time.Ticker
	queryGoods             chan struct{}
	queryAccount           chan struct{}
	queryBenefits          chan struct{}
	querySpendTransactions chan struct{}
	queryBalance           chan struct{}
	queryOrders            chan struct{}
	caculateUserBenefit    chan struct{}
	persistentResult       chan struct{}

	goodAccountings []*goodAccounting
}

func (ac *accounting) onScheduleTick() {
	go func() { ac.queryGoods <- struct{}{} }()
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
			good: good,
		})
	}
	ac.goodAccountings = acs

	go func() { ac.queryAccount <- struct{}{} }()
}

func (ac *accounting) onQueryAccount(ctx context.Context) {
	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetPlatformSettingByGood(ctx, &billingpb.GetPlatformSettingByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get platform setting by good: %v", err)
			continue
		}

		gac.goodsetting = resp.Info
	}

	go func() { ac.queryBenefits <- struct{}{} }()
}

func (ac *accounting) onQueryBenefits(ctx context.Context) {
	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetPlatformBenefitsByGood(ctx, &billingpb.GetPlatformBenefitsByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get platform benefits by good: %v", err)
			continue
		}

		gac.benefits = resp.Infos
	}

	go func() { ac.querySpendTransactions <- struct{}{} }()
}

func (ac *accounting) onQuerySpendTransactions(ctx context.Context) {
	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetCoinAccountTransactionsByCoinAccount(ctx, &billingpb.GetCoinAccountTransactionsByCoinAccountRequest{
			CoinTypeID: gac.good.CoinInfoID,
			AddressID:  gac.goodsetting.BenefitAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get benefit account transaction by good: %v", err)
			continue
		}

		for _, info := range resp.Infos {
			if info.ToAddressID == gac.goodsetting.BenefitAccountID {
				logger.Sugar().Errorf("good benefit account should not accept platform incoming transaction: %v", info.ToAddressID)
				continue
			}
		}

		gac.transactions = resp.Infos
	}

	go func() { ac.queryBalance <- struct{}{} }()
}

func (ac *accounting) onQueryBalance(ctx context.Context) {
}

func (ac *accounting) onQueryOrders(ctx context.Context) {
}

func (ac *accounting) onCaculateUserBenefit(ctx context.Context) {
}

func (ac *accounting) onPersistentResult(ctx context.Context) {
}

func Run(ctx context.Context) {
	ac := &accounting{
		ticker:                 time.NewTicker(3 * time.Second),
		queryGoods:             make(chan struct{}),
		queryAccount:           make(chan struct{}),
		queryBenefits:          make(chan struct{}),
		querySpendTransactions: make(chan struct{}),
		queryBalance:           make(chan struct{}),
		queryOrders:            make(chan struct{}),
		caculateUserBenefit:    make(chan struct{}),
		persistentResult:       make(chan struct{}),
	}

	for {
		select {
		case <-ac.ticker.C:
			ac.onScheduleTick()
		case <-ac.queryGoods:
			ac.onQueryGoods(ctx)
		case <-ac.queryAccount:
			ac.onQueryAccount(ctx)
		case <-ac.queryBenefits:
			ac.onQueryBenefits(ctx)
		case <-ac.querySpendTransactions:
			ac.onQuerySpendTransactions(ctx)
		case <-ac.queryBalance:
			ac.onQueryBalance(ctx)
		case <-ac.queryOrders:
			ac.onQueryOrders(ctx)
		case <-ac.caculateUserBenefit:
			ac.onCaculateUserBenefit(ctx)
		case <-ac.persistentResult:
			ac.onPersistentResult(ctx)
		}
	}
}
