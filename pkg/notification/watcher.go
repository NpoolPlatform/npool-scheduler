package notification

import (
	"context"
	"time"
)

func send(ctx context.Context) {

}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			processTx(ctx)
			send(ctx)
		case <-ctx.Done():
			return
		}
	}
}
