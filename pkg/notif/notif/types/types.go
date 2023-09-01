package types

import (
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
)

type PersistentNotif struct {
	*notifmwpb.Notif
}
