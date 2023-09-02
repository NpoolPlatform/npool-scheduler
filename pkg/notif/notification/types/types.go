package types

import (
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	sendmwpb "github.com/NpoolPlatform/message/npool/third/mw/v1/send"
)

type PersistentNotif struct {
	*notifmwpb.Notif
	EventNotifs    []*notifmwpb.Notif
	MessageRequest *sendmwpb.SendMessageRequest
}
