package goodbenefit

import (
	"context"
	"time"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			send(ctx, basetypes.NotifChannel_ChannelEmail)
		case <-ctx.Done():
			return
		}
	}
}
