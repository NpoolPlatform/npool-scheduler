package notification

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
			processTx(ctx)
			send(ctx, basetypes.NotifChannel_ChannelEmail)
			send(ctx, basetypes.NotifChannel_ChannelSMS)
		case <-ctx.Done():
			return
		}
	}
}
