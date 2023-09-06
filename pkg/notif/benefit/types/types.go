package types

import (
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
)

type NotifContent struct {
	AppID   string
	Content string
}

type PersistentGoodBenefit struct {
	Benefits      []*notifbenefitmwpb.GoodBenefit
	NotifContents []*NotifContent
}
