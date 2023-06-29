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
	benefitInterval = 24 * time.Hour
	checkDelay      = 6 * time.Hour
)

func prepareInterval() {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_GOOD_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		benefitInterval = duration
	}
	if delay, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_GOOD_BENEFIT_CHECK_DELAY_SECONDS"))); err == nil {
		checkDelay = delay
	}
}

func nextBenefitAt() time.Time {
	now := time.Now()
	nowSec := now.Unix()
	benefitSeconds := int64(benefitInterval.Seconds())
	nextSec := (nowSec+benefitSeconds)/benefitSeconds*benefitSeconds + int64(checkDelay.Seconds())
	return now.Add(time.Duration(nextSec-nowSec) * time.Second)
}

func delay() {
	start := nextBenefitAt()
	logger.Sugar().Infow("delay", "startAfter", time.Until(start).Seconds(), "start", start)
	<-time.After(time.Until(start))
}

func Watch(ctx context.Context) {
	prepareInterval()
	logger.Sugar().Infow(
		"goodbenefit",
		"GoodBenefitIntervalSeconds", benefitInterval,
	)

	delay()

	tickerWait := time.NewTicker(benefitInterval)

	for { //nolint
		select {
		case <-tickerWait.C:
			logger.Sugar().Infow(
				"Watch",
				"State", "good benefit ticker start",
			)
			send(ctx, basetypes.NotifChannel_ChannelEmail)
		}
	}
}
