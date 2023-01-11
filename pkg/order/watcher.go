package order

import (
	"context"
	"time"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		checkOrderPayments(ctx)
		checkOrderStart(ctx)
		checkOrderExpiries(ctx)
	}
}
