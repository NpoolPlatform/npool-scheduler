package goodbenefit

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
)

var (
	benefitInterval  = 24 * time.Hour
	firstCheckDelay  = 1 * time.Hour
	secondCheckDelay = 6 * time.Hour
)

func prepareInterval() {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_GOOD_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		benefitInterval = duration
	}
	if firstDelay, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_GOOD_BENEFIT_CHECK_FIRST_DELAY_SECONDS"))); err == nil {
		firstCheckDelay = firstDelay
	}
	if delay, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_GOOD_BENEFIT_CHECK_DELAY_SECONDS"))); err == nil {
		secondCheckDelay = delay
	}
}

func nextBenefitAt(delay time.Duration) time.Time {
	now := time.Now()
	nowSec := now.Unix()
	benefitSeconds := int64(benefitInterval.Seconds())
	nextSec := (nowSec+benefitSeconds)/benefitSeconds*benefitSeconds + int64(delay.Seconds())
	return now.Add(time.Duration(nextSec-nowSec) * time.Second)
}

func delay(delay time.Duration) {
	start := nextBenefitAt(delay)
	logger.Sugar().Infow("delay", "startAfter", time.Until(start).Seconds(), "start", start)
	<-time.After(time.Until(start))
}

func Watch(ctx context.Context) {
	prepareInterval()
	logger.Sugar().Infow(
		"goodbenefit",
		"GoodBenefitIntervalSeconds", benefitInterval,
	)

	delay(firstCheckDelay)
	send(ctx, basetypes.NotifChannel_ChannelEmail)
	firstTickerWait := time.NewTicker(benefitInterval)

	delay(secondCheckDelay)
	send(ctx, basetypes.NotifChannel_ChannelEmail)
	secondTickerWait := time.NewTicker(benefitInterval)

	for { //nolint
		select {
		case <-firstTickerWait.C:
			logger.Sugar().Infow(
				"Watch",
				"State", "good benefit first ticker start",
			)
			send(ctx, basetypes.NotifChannel_ChannelEmail)
		case <-secondTickerWait.C:
			logger.Sugar().Infow(
				"Watch",
				"State", "good benefit second ticker start",
			)
			send(ctx, basetypes.NotifChannel_ChannelEmail)
		}
	}
}
