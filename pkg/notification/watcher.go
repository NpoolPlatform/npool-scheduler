package notification

import (
	"context"
	"time"

	chanmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/channel"
)

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			processTx(ctx)
			send(ctx, chanmgrpb.NotifChannel_ChannelEmail)
			send(ctx, chanmgrpb.NotifChannel_ChannelSMS)
		case <-ctx.Done():
			return
		}
	}
}
