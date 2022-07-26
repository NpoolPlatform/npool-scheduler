package benefit

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	goodscli "github.com/NpoolPlatform/cloud-hashing-goods/pkg/client"
	goodspb "github.com/NpoolPlatform/message/npool/cloud-hashing-goods"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	"github.com/shopspring/decimal"
)

var benefitInterval = 24 * time.Hour

func interval() time.Duration {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		return duration
	}
	return benefitInterval
}

func dayStart() time.Time {
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, now.Location())
}

func delay() {
	start := dayStart()
	if time.Until(start) > benefitInterval {
		start = time.Now().Add(benefitInterval)
	}

	logger.Sugar().Infow("delay", "startAfter", time.Until(start))

	<-time.After(time.Until(start))
}

func processGood(ctx context.Context, good *goodspb.GoodInfo, timestamp time.Time) error {
	coin, err := coininfocli.GetCoinInfo(ctx, good.CoinInfoID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	setting, err := billingcli.GetCoinSetting(ctx, good.CoinInfoID)
	if err != nil {
		return err
	}
	if setting == nil {
		return fmt.Errorf("invalid coin setting")
	}

	offset := int32(0)
	limit := int32(1000)

	_gp := &gp{
		goodID:     good.ID,
		coinTypeID: coin.ID,
		coinName:   coin.Name,
	}

	if err := _gp.processDailyProfit(ctx, timestamp); err != nil {
		return err
	}
	if _gp.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	for {
		orders, err := ordercli.GetGoodOrders(ctx, good.ID, offset, limit)
		if err != nil {
			return err
		}

		for _, order := range orders {
			if err := _gp.processOrder(ctx, order, timestamp); err != nil {
				return err
			}
		}

		offset += limit
	}
}

func processGoods(ctx context.Context, timestamp time.Time) {
	goods, err := goodscli.GetGoods(ctx)
	if err != nil {
		logger.Sugar().Errorw("processGoods", "error", err)
		return
	}

	for _, good := range goods {
		if err := processGood(ctx, good, timestamp); err != nil {
			logger.Sugar().Errorw("processGoods", "goodID", good.ID, "goodName", good.Title, "error", err)
			return
		}
	}
}

func Watch(ctx context.Context) {
	benefitInterval = interval()
	logger.Sugar().Infow("benefit", "intervalSeconds", benefitInterval)

	delay()

	ticker := time.NewTicker(benefitInterval)
	for {
		timestamp := dayStart()
		processGoods(ctx, timestamp)
		<-ticker.C
	}
}
