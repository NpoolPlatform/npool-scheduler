package currency

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	currency "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency"
	fiat "github.com/NpoolPlatform/chain-middleware/pkg/client/fiat"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)

	const fiatInterval = 60
	fiatTicker := 0

	for range ticker.C {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		if err := currency.RefreshCurrencies(ctx); err != nil {
			logger.Sugar().Errorw("currency", "error", err)
		}

		if fiatTicker == fiatInterval {
			if err := fiat.RefreshFiatCurrencies(ctx); err != nil {
				logger.Sugar().Errorw("currency", "error", err)
			}
		}

		fiatTicker++

		cancel()
	}
}
