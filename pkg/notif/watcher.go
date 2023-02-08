package notif

import (
	"context"
)

func Watch(ctx context.Context) {
	sendAnnouncement(ctx)
	//ticker := time.NewTicker(30 * time.Second)
	//for range ticker.C {
	//	sendNotif(ctx)
	//	sendAnnouncement(ctx)
	//}
}
