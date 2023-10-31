package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	ancmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	ancmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/types"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) scanAnnouncement(ctx context.Context, channel basetypes.NotifChannel, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	now := uint32(time.Now().Unix())

	for {
		ancs, _, err := ancmwcli.GetAnnouncements(ctx, &ancmwpb.Conds{
			StartAt: &basetypes.Uint32Val{Op: cruder.LTE, Value: now},
			EndAt:   &basetypes.Uint32Val{Op: cruder.GTE, Value: now},
			Channel: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(channel)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(ancs) == 0 {
			return nil
		}

		for _, anc := range ancs {
			cancelablefeed.CancelableFeed(ctx, anc, exec)
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if err := h.scanAnnouncement(ctx, basetypes.NotifChannel_ChannelEmail, exec); err != nil {
		return err
	}
	return h.scanAnnouncement(ctx, basetypes.NotifChannel_ChannelSMS, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	if announcement, ok := ent.(*types.PersistentAnnouncement); ok {
		return announcement.EntID
	}
	return ent.(*ancmwpb.Announcement).EntID
}
