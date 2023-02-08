package announcement

import (
	"context"
	"time"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	for range ticker.C {
		sendAnnouncement(ctx)
	}
}
