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
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	"github.com/shopspring/decimal"
)

var benefitInterval = 24 * time.Hour

const secondsPerDay = 24 * 60 * 60

func interval() time.Duration {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		return duration
	}
	return benefitInterval
}

func todayStart() time.Time {
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, now.Location())
}

func tomorrowStart() time.Time {
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, now.Location())
}

func delay() {
	start := tomorrowStart()
	if time.Until(start) > benefitInterval {
		start = time.Now().Add(benefitInterval)
	}

	logger.Sugar().Infow("delay", "startAfter", time.Until(start))

	<-time.After(time.Until(start))
}

func validateGoodOrder(ctx context.Context, order *orderpb.Order, timestamp time.Time) (bool, error) {
	payment, err := ordercli.GetOrderPayment(ctx, order.ID)
	if err != nil {
		return false, err
	}
	if payment == nil {
		return false, nil
	}
	if payment.State != orderconst.PaymentStateDone {
		return false, nil
	}

	orderEnd := order.CreateAt + secondsPerDay
	if orderEnd < uint32(time.Now().Unix()) {
		return false, nil
	}
	if order.Start > uint32(time.Now().Unix()) {
		return false, nil
	}

	return true, nil
}

func processGood(ctx context.Context, good *goodspb.GoodInfo, timestamp time.Time) error {
	if good.StartAt > uint32(time.Now().Unix()) {
		return nil
	}

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
		goodName:   good.Title,
		coinTypeID: coin.ID,
		coinName:   coin.Name,
	}

	if err := _gp.processDailyProfit(ctx, timestamp); err != nil {
		return err
	}

	logger.Sugar().Infow("benefit", "timestamp", timestamp, "goodID", good.ID, "goodName", good.Title, "profit", _gp.dailyProfit)

	if _gp.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	if err := gp.stock(ctx); err != nil {
		return err
	}

	for {
		orders, err := ordercli.GetGoodOrders(ctx, good.ID, offset, limit)
		if err != nil {
			return err
		}

		for _, order := range orders {
			validate, err := validateGoodOrder(ctx, order, timestamp)
			if err != nil {
				return err
			}
			if !validate {
				continue
			}
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
		}
	}
}

func Watch(ctx context.Context) {
	benefitInterval = interval()
	logger.Sugar().Infow("benefit", "intervalSeconds", benefitInterval)

	delay()

	ticker := time.NewTicker(benefitInterval)
	for {
		timestamp := todayStart()
		logger.Sugar().Infow("benefit", "timestamp", timestamp)
		processGoods(ctx, timestamp)
		<-ticker.C
	}
}
