package executor

import (
	"context"
	"fmt"
	"net/mail"
	"regexp"
	"time"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	applangmwcli "github.com/NpoolPlatform/g11n-middleware/pkg/client/applang"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	usermwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	applangmwpb "github.com/NpoolPlatform/message/npool/g11n/mw/v1/applang"
	ancmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	ancsendmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement/sendstate"
	ancusermwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement/user"
	emailtmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/email"
	smstmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/sms"
	sendmwpb "github.com/NpoolPlatform/message/npool/third/mw/v1/send"
	ancsendmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement/sendstate"
	ancusermwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement/user"
	emailtmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/email"
	smstmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/sms"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/types"
)

type announcementHandler struct {
	*ancmwpb.Announcement
	persistent chan interface{}
	done       chan interface{}
	sendStats  map[string]*ancsendmwpb.SendState
}

func (h *announcementHandler) getSendStats(ctx context.Context, users []*usermwpb.User) error {
	uids := []string{}
	for _, user := range users {
		uids = append(uids, user.ID)
	}
	stats, _, err := ancsendmwcli.GetSendStates(ctx, &ancsendmwpb.Conds{
		AppID:          &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		AnnouncementID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
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

func (h *announcementHandler) getLang(ctx context.Context, user *usermwpb.User) (*applangmwpb.Lang, error) {
	conds := &applangmwpb.Conds{
		AppID: &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
	}
	if user.SelectedLangID != nil {
		conds.LangID = &basetypes.StringVal{Op: cruder.EQ, Value: *user.SelectedLangID}
	} else {
		conds.Main = &basetypes.BoolVal{Op: cruder.EQ, Value: true}
	}
	lang, err := applangmwcli.GetLangOnly(ctx, conds)
	if err != nil {
		return nil, err
	}
	if lang != nil {
		return lang, nil
	}
	if user.SelectedLangID == nil {
		return nil, fmt.Errorf("invalid mainlang")
	}
	conds.LangID = nil
	conds.Main = &basetypes.BoolVal{Op: cruder.EQ, Value: true}
	lang, err = applangmwcli.GetLangOnly(ctx, conds)
	if err != nil {
		return nil, err
	}
	if lang == nil {
		return nil, fmt.Errorf("invalid mainlang")
	}
	return lang, nil
}

func (h *announcementHandler) emailRequest(ctx context.Context, user *usermwpb.User) (*sendmwpb.SendMessageRequest, error) {
	req := &sendmwpb.SendMessageRequest{
		Subject: h.Title,
		Content: h.Content,
	}

	tmpl, err := emailtmplmwcli.GetEmailTemplateOnly(ctx, &emailtmplmwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.LangID},
		UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(basetypes.UsedFor_Announcement)},
	})
	if err != nil {
		return nil, err
	}
	if tmpl == nil {
		return nil, fmt.Errorf("invalid template")
	}

	req.From = tmpl.Sender
	req.To = user.EmailAddress
	req.ToCCs = tmpl.CCTos
	req.ReplyTos = tmpl.ReplyTos
	req.AccountType = basetypes.SignMethod_Email

	return req, nil
}

func (h *announcementHandler) smsRequest(ctx context.Context, user *usermwpb.User) (*sendmwpb.SendMessageRequest, error) {
	req := &sendmwpb.SendMessageRequest{
		Subject: h.Title,
		Content: h.Content,
	}

	tmpl, err := smstmplmwcli.GetSMSTemplateOnly(ctx, &smstmplmwpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.LangID},
		UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(basetypes.UsedFor_Announcement)},
	})
	if err != nil {
		return nil, err
	}
	if tmpl == nil {
		return nil, fmt.Errorf("invalid template")
	}

	req.To = user.PhoneNO
	req.AccountType = basetypes.SignMethod_Mobile
	return req, nil
}

func (h *announcementHandler) unicast(ctx context.Context, user *usermwpb.User) error {
	if _, ok := h.sendStats[user.ID]; ok {
		return nil
	}

	switch h.Channel {
	case basetypes.NotifChannel_ChannelEmail:
		if _, err := mail.ParseAddress(user.EmailAddress); err != nil {
			return nil
		}
	case basetypes.NotifChannel_ChannelSMS:
		re := regexp.MustCompile(
			`^(?:(?:\(?(?:00|\+)([1-4]\d\d|[1-9]\d?)\)?)?[` +
				`\-\.\ \\\/]?)?((?:\(?\d{1,}\)?[\-\.\ \\\/]?)` +
				`{0,})(?:[\-\.\ \\\/]?(?:#|ext\.?|extension|x)` +
				`[\-\.\ \\\/]?(\d+))?$`,
		)
		if !re.MatchString(user.PhoneNO) {
			return nil
		}
	default:
		return nil
	}

	lang, err := h.getLang(ctx, user)
	if err != nil {
		return err
	}
	if lang.LangID != h.LangID {
		return nil
	}

	var req *sendmwpb.SendMessageRequest
	switch h.Channel {
	case basetypes.NotifChannel_ChannelEmail:
		if req, err = h.emailRequest(ctx, user); err != nil {
			return err
		}
	case basetypes.NotifChannel_ChannelSMS:
		if req, err = h.smsRequest(ctx, user); err != nil {
			return err
		}
	}

	asyncfeed.AsyncFeed(ctx, &types.PersistentAnnouncement{
		Announcement:   h.Announcement,
		SendAppID:      user.AppID,
		SendUserID:     user.ID,
		MessageRequest: req,
	}, h.persistent)

	return nil
}

func (h *announcementHandler) multicastUsers(ctx context.Context, users []*usermwpb.User) error {
	if err := h.getSendStats(ctx, users); err != nil {
		return err
	}
	for _, user := range users {
		if err := h.unicast(ctx, user); err != nil {
			logger.Sugar().Errorw(
				"multicastUsers",
				"AnnouncementID", h.Announcement.ID,
				"User", user,
				"Error", err,
			)
			return err
		}
		time.Sleep(100 * time.Millisecond)
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
			return nil
		}

		if err := h.multicastUsers(ctx, users); err != nil {
			logger.Sugar().Errorw(
				"broadcast",
				"AnnouncementID", h.Announcement.ID,
				"Error", err,
			)
			return err
		}

		offset += limit
	}
}

func (h *announcementHandler) multicast(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		ancUsers, _, err := ancusermwcli.GetAnnouncementUsers(ctx, &ancusermwpb.Conds{
			AppID:          &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
			AnnouncementID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
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

func (h *announcementHandler) exec(ctx context.Context) error {
	h.sendStats = map[string]*ancsendmwpb.SendState{}

	defer asyncfeed.AsyncFeed(ctx, h.Announcement, h.done)

	switch h.AnnouncementType {
	case basetypes.NotifType_NotifBroadcast:
		if err := h.broadcast(ctx); err != nil {
			return err
		}
	case basetypes.NotifType_NotifMulticast:
		if err := h.multicast(ctx); err != nil {
			return err
		}
	}

	return nil
}
