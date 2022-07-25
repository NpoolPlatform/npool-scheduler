package benefit

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
)

var (
	benefitInterval = 24 * time.Hour
)

func interval() time.Duration {
	if duration, err := time.ParseDuration(fmt.Sprintf("%vs", os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		return duration
	}
	return benefitInterval
}

func delay() {
	now := time.Now()
	y, m, d := now.Date()
	start := time.Date(y, m, d+1, 0, 0, 0, 0, now.Location())

	if time.Until(start) > benefitInterval {
		start = now.Add(benefitInterval)
	}

	logger.Sugar().Infow("delay", "startAfter", time.Until(start))

	<-time.After(time.Until(start))
}

func Watch(ctx context.Context) {
	benefitInterval = interval()
	logger.Sugar().Infow("benefit", "intervalSeconds", benefitInterval)

	delay()

	ticker := time.NewTicker(benefitInterval)
	for {
		<-ticker.C
	}
}
