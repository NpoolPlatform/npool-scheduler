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
	return nil
}
