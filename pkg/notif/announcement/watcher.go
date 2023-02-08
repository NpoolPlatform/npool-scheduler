package announcement

import (
	"context"
)

func Watch(ctx context.Context) {
	sendAnnouncement(ctx)
}
