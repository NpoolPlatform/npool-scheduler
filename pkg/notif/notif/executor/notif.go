package executor

import (
	"context"

	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
)

type notifHandler struct {
	*notifmwpb.Notif
	persistent chan interface{}
}

func (h *notifHandler) exec(ctx context.Context) error {
	// 获取LangID
	// 根据Channel(Email|SMS)获取各自的模版,生成对应的SendMessage的Req
	// 发送消息
	// 发送消息后,需要将该消息的其它语言通知
	return nil
}
