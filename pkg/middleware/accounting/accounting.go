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
	good        *goodspb.GoodInfo
	goodsetting *billingpb.PlatformSetting
}

type accounting struct {
	ticker              *time.Ticker
	queryGoods          chan struct{}
	queryAccounts       chan struct{}
	queryBalance        chan struct{}
	queryOrders         chan struct{}
	caculateUserBenefit chan struct{}
	persistentResult    chan struct{}

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

	go func() { ac.queryAccounts <- struct{}{} }()
}

func (ac *accounting) onQueryAccounts(ctx context.Context) {
	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetPlatformSettingByGood(ctx, &billingpb.GetPlatformSettingByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get platform setting by good: %v", err)
			return
		}

		logger.Sugar().Infof("get good setting: %v", resp.Info)
		gac.goodsetting = resp.Info
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
		ticker:              time.NewTicker(3 * time.Second),
		queryGoods:          make(chan struct{}),
		queryAccounts:       make(chan struct{}),
		queryBalance:        make(chan struct{}),
		queryOrders:         make(chan struct{}),
		caculateUserBenefit: make(chan struct{}),
		persistentResult:    make(chan struct{}),
	}

	for {
		select {
		case <-ac.ticker.C:
			ac.onScheduleTick()
		case <-ac.queryGoods:
			ac.onQueryGoods(ctx)
		case <-ac.queryAccounts:
			ac.onQueryAccounts(ctx)
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
