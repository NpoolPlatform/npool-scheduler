package goodbenefit

import (
	"context"
	"time"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
)

func Watch(ctx context.Context) {
	hour := 6
	minute := 0
	second := 0
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, 0, now.Location())
	if now.After(next) {
		next = next.Add(24 * time.Hour)
	}

	duration := next.Sub(now)
	timer := time.NewTicker(duration)
	defer timer.Stop()

	for { //nolint
		select {
		case <-timer.C:
			send(ctx, basetypes.NotifChannel_ChannelEmail)
			now := time.Now()
			next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, 0, now.Location())
			next = next.Add(24 * time.Hour)
			duration = next.Sub(now)
			timer.Stop()
			timer = time.NewTicker(duration)
		}
	}
}
