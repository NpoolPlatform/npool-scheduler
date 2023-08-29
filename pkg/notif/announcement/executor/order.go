package executor

import (
	"context"
	"fmt"
	"time"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	ancmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/types"
)

type announcementHandler struct {
	*announcementmwpb.Announcement
	persistent chan interface{}
	sendStats  map[string]*ancsendmwpb.SendState
}

func (h *announcementHandler) getSendStats(ctx context.Context, users []*usermwpb.User) error {
	uids := []string{}
	for _, user := range users {
		uids = append(uids, user.ID)
	}
	stats, _, err := ancsendmwcli.GetSendStates(ctx, &ancsendmwpb.Conds{
		AppID:          &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		AnnouncementID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		Channel:        &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(h.Channel)},
		UserIDs:        &basetypes.StringSliceVal{Op: cruder.IN, Value: uids},
	}, 0, int32(len(uids)))
	if err != nil {
		return err
	}
	for _, stat := range stats {
		h.sendStats[stat.UserID] = stat
	}
	return nil
}

func (h *announcementHandler) multicastUsers(ctx context.Context, users []*usermwpb.User) error {
	if err := h.getSendStats(ctx, users); err != nil {
		return err
	}

	for _, user := range users {

	}

	return nil
}

func (h *announcementHandler) broadcast(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		users, _, err := usermwcli.GetUsers(ctx, &usermwpb.Conds{
			AppID: &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(users) == 0 {
			break
		}

		if err := multicastUsers(ctx, users); err != nil {
			return err
		}

		offset += limit
	}
}

func (h *announcementHandler) multicast(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		ancUsers, _, err := ancusermwcli.GetAnnouncementUsers(ctx, &ancusermgrpb.Conds{
			AppID:          &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
			AnnouncementID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(ancUsers) == 0 {
			return nil
		}

		offset += limit

		uids := []string{}
		for _, user := range ancUsers {
			uids = append(uids, user.UserID)
		}

		users, _, err := usermwcli.GetUsers(ctx, &usermwpb.Conds{
			IDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: uids},
		}, 0, int32(len(uids)))
		if err != nil {
			return err
		}
		if len(users) == 0 {
			continue
		}

		if err := h.multicastUsers(ctx, users); err != nil {
			return err
		}
	}
}

func (h *announcementHandler) final() {
	persistentAnnouncement := &types.PersistentAnnouncement{
		Announcement: h.Announcement,
	}
	h.persistent <- persistentAnnouncement
}

func (h *announcementHandler) exec(ctx context.Context) error {
	h.sendStates = map[string]*ancsendmwpb.SendState{}

	var err error
	defer h.final()

	switch h.AnnouncementType {
	case basetypes.NotifType_NotifBroadcast:
		if err = h.broadcast(ctx); err != nil {
			return err
		}
	case basetypes.NotifType_NotifMulticast:
		if err = h.broadcast(ctx); err != nil {
			return err
		}
	}

	return nil
}
