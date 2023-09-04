package cancelablefeed

import (
	"context"
)

func CancelableFeed(ctx context.Context, ent interface{}, ch chan interface{}) {
	select {
	case <-ctx.Done():
	case ch <- ent:
	}
}
