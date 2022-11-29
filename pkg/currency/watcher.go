package currency

import (
	"context"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"time"

	currencyvalue "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency/value"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		err := currencyvalue.CreateCurrencies(ctx)
		if err != nil {
			logger.Sugar().Errorw("currency", "error", err)
			return
		}
	}
}
