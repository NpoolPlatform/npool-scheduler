package currency

import (
	"context"
	"time"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		saveCurrency(ctx)
	}
}
