package fiatcurrency

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	currency "github.com/NpoolPlatform/chain-middleware/pkg/client/fiat"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Minute)

	for range ticker.C {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		if err := currency.RefreshFiatCurrencies(ctx); err != nil {
			logger.Sugar().Errorw("currency", "error", err)
		}
		cancel()
	}
}
