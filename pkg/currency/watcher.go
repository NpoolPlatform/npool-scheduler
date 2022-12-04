package currency

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	currency "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency"
)

func Watch(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	ticker := time.NewTicker(1 * time.Minute)

	defer cancel()

	for range ticker.C {
		if err := currency.RefreshCurrencies(ctx); err != nil {
			logger.Sugar().Errorw("currency", "error", err)
		}
	}
}
