package types

import (
	ancmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	sendmwpb "github.com/NpoolPlatform/message/npool/third/mw/v1/send"
)

type PersistentAnnouncement struct {
	*ancmwpb.Announcement
	SendAppID      string
	SendUserID     string
	MessageRequest *sendmwpb.SendMessageRequest
}
