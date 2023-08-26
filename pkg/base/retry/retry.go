package retry

import (
	"context"
	"time"
)

func Retry(ctx context.Context, ent interface{}, retry chan interface{}) {
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(time.Minute):
			retry <- ent
		}
	}()
}
