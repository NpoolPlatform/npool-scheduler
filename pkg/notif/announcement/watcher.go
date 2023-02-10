package announcement

import (
	"context"
	"time"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		sendAnnouncement(ctx)
	}
}
