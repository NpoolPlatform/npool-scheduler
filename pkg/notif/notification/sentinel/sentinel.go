package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) scanNotification(ctx context.Context, channel basetypes.NotifChannel, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		notifs, _, err := notifmwcli.GetNotifs(ctx, &notifmwpb.Conds{
			Notified: &basetypes.BoolVal{Op: cruder.EQ, Value: false},
			Channel:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(channel)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(notifs) == 0 {
			break
		}

		for _, notif := range notifs {
			exec <- notif
			time.Sleep(100 * time.Millisecond)
		}

		offset += limit
	}
	return nil
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if err := h.scanNotification(ctx, basetypes.NotifChannel_ChannelEmail, exec); err != nil {
		return err
	}
	return h.scanNotification(ctx, basetypes.NotifChannel_ChannelSMS, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	return ent.(*notifmwpb.Notif).ID
}