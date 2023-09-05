package asyncfeed

import (
	"context"
)

func AsyncFeed(ctx context.Context, ent interface{}, ch chan interface{}) {
	go func() {
		select {
		case <-ctx.Done():
		case ch <- ent:
		}
	}()
}
