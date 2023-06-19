package announcement

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	usermwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	applangmwpb "github.com/NpoolPlatform/message/npool/g11n/mw/v1/applang"
	ancmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	ancsendmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement/sendstate"
	ancusermgrpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement/user"
	emailtmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/email"
	smstmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/sms"
	sendmwpb "github.com/NpoolPlatform/message/npool/third/mw/v1/send"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	applangmwcli "github.com/NpoolPlatform/g11n-middleware/pkg/client/applang"
	ancmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement"
	ancsendmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement/sendstate"
	ancusermwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement/user"
	emailtmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/email"
	smstmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/sms"
	sendmwcli "github.com/NpoolPlatform/third-middleware/pkg/client/send"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
)

func unicast(ctx context.Context, anc *ancmwpb.Announcement, user *usermwpb.User) (bool, error) {
	req := &sendmwpb.SendMessageRequest{
		Subject: anc.Title,
		Content: anc.Content,
	}

	langConds := &applangmwpb.Conds{
		AppID: &basetypes.StringVal{Op: cruder.EQ, Value: anc.AppID},
	}
	if user.SelectedLangID != nil {
		langConds.LangID = &basetypes.StringVal{Op: cruder.EQ, Value: *user.SelectedLangID}
	} else {
		langConds.Main = &basetypes.BoolVal{Op: cruder.EQ, Value: true}
	}

	lang, err := applangmwcli.GetLangOnly(ctx, langConds)
	if err != nil {
		return false, err
	}
	if lang == nil {
		return false, fmt.Errorf("app %v main lang invalid", anc.AppID)
	}

	if lang.LangID != anc.LangID {
		return false, nil
	}

	switch anc.Channel {
	case basetypes.NotifChannel_ChannelEmail:
		tmpl, err := emailtmplmwcli.GetEmailTemplateOnly(ctx, &emailtmplmwpb.Conds{
			AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: anc.AppID},
			LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: anc.LangID},
			UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(basetypes.UsedFor_Announcement)},
		})
		if err != nil {
			return false, err
		}
		if tmpl == nil {
			return false, fmt.Errorf("app %v lang %v email template invalid", anc.AppID, anc.LangID)
		}

		req.From = tmpl.Sender
		req.To = user.EmailAddress
		req.ToCCs = tmpl.CCTos
		req.ReplyTos = tmpl.ReplyTos
		req.AccountType = basetypes.SignMethod_Email
	case basetypes.NotifChannel_ChannelSMS:
		tmpl, err := smstmplmwcli.GetSMSTemplateOnly(ctx, &smstmplmwpb.Conds{
			AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: anc.AppID},
			LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: anc.LangID},
			UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(basetypes.UsedFor_Announcement)},
		})
		if err != nil {
			return false, err
		}
		if tmpl == nil {
			return false, fmt.Errorf("app %v lang %v sms template invalid", anc.AppID, anc.LangID)
		}

		req.To = user.PhoneNO
		req.AccountType = basetypes.SignMethod_Mobile
	}

	logger.Sugar().Infow(
		"unicast",
		"AppID", user.AppID,
		"UserID", user.ID,
		"EmailAddress", user.EmailAddress,
		"AnnouncementID", anc.ID,
		"AnnoucementType", anc.AnnouncementType,
		"State", "Sending")
	if err := sendmwcli.SendMessage(ctx, req); err != nil {
		return false, err
	}
	return true, nil
}

func multicastUsers(ctx context.Context, anc *ancmwpb.Announcement, users []*usermwpb.User) error {
	uids := []string{}
	for _, user := range users {
		uids = append(uids, user.ID)
	}

	stats, _, err := ancsendmwcli.GetSendStates(ctx, &ancsendmwpb.Conds{
		AppID:          &basetypes.StringVal{Op: cruder.EQ, Value: anc.AppID},
		AnnouncementID: &basetypes.StringVal{Op: cruder.EQ, Value: anc.ID},
		Channel:        &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(anc.Channel.Number())},
		UserIDs:        &basetypes.StringSliceVal{Op: cruder.IN, Value: uids},
	}, 0, int32(len(uids)))
	if err != nil {
		return err
	}

	statMap := map[string]*ancsendmwpb.SendState{}
	for _, stat := range stats {
		statMap[stat.UserID] = stat
	}

	statReqs := []*ancsendmwpb.SendStateReq{}

	for _, user := range users {
		if _, ok := statMap[user.ID]; ok {
			logger.Sugar().Infow(
				"multicastUsers",
				"AppID", user.AppID,
				"UserID", user.ID,
				"EmailAddress", user.EmailAddress,
				"AnnouncementID", anc.ID,
				"AnnoucementType", anc.AnnouncementType,
				"State", "Sent")
			continue
		}

		switch anc.Channel {
		case basetypes.NotifChannel_ChannelEmail:
			if !strings.Contains(user.EmailAddress, "@") {
				logger.Sugar().Errorw(
					"multicastUsers",
					"AppID", user.AppID,
					"UserID", user.ID,
					"EmailAddress", user.EmailAddress,
					"State", "Invalid")
				continue
			}
		case basetypes.NotifChannel_ChannelSMS:
			if user.PhoneNO == "" {
				logger.Sugar().Errorw(
					"multicastUsers",
					"AppID", user.AppID,
					"UserID", user.ID,
					"PhoneNO", user.PhoneNO,
					"State", "Invalid")
				continue
			}
		default:
			continue
		}

		sent, err := unicast(ctx, anc, user)
		if err != nil {
			logger.Sugar().Errorw(
				"multicastUsers",
				"AppID", user.AppID,
				"UserID", user.ID,
				"EmailAddress", user.EmailAddress,
				"PhoneNO", user.PhoneNO,
				"error", err)
			break
		}

		if !sent {
			continue
		}

		statReqs = append(statReqs, &ancsendmwpb.SendStateReq{
			AppID:          &anc.AppID,
			UserID:         &user.ID,
			AnnouncementID: &anc.ID,
			Channel:        &anc.Channel,
		})
	}

	if len(statReqs) == 0 {
		return nil
	}

	if _, err := ancsendmwcli.CreateSendStates(ctx, statReqs); err != nil {
		return err
	}

	return nil
}

func broadcast(ctx context.Context, anc *ancmwpb.Announcement) error {
	offset := int32(0)
	limit := int32(1000)

	for {
		users, _, err := usermwcli.GetUsers(ctx, &usermwpb.Conds{
			AppID: &basetypes.StringVal{Op: cruder.EQ, Value: anc.AppID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(users) == 0 {
			break
		}

		if err := multicastUsers(ctx, anc, users); err != nil {
			return err
		}

		offset += limit
	}

	return nil
}

func multicast(ctx context.Context, anc *ancmwpb.Announcement) error {
	offset := int32(0)
	limit := int32(1000)

	for {
		ancUsers, _, err := ancusermwcli.GetAnnouncementUsers(ctx, &ancusermgrpb.Conds{
			AppID:          &basetypes.StringVal{Op: cruder.EQ, Value: anc.AppID},
			AnnouncementID: &basetypes.StringVal{Op: cruder.EQ, Value: anc.ID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(ancUsers) == 0 {
			break
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

		if err := multicastUsers(ctx, anc, users); err != nil {
			return err
		}
	}

	return nil
}

func sendOne(ctx context.Context, anc *ancmwpb.Announcement) error {
	switch anc.AnnouncementType {
	case ancmwpb.AnnouncementType_Broadcast:
		return broadcast(ctx, anc)
	case ancmwpb.AnnouncementType_Multicast:
		return multicast(ctx, anc)
	}
	return fmt.Errorf("announcement invalid")
}

func send(ctx context.Context, channel basetypes.NotifChannel) {
	offset := int32(0)
	limit := int32(100)
	now := uint32(time.Now().Unix())

	for {
		ancs, _, err := ancmwcli.GetAnnouncements(ctx, &ancmwpb.Conds{
			EndAt:   &basetypes.Uint32Val{Op: cruder.GT, Value: now},
			Channel: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(channel)},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("send", "error", err)
			return
		}
		if len(ancs) == 0 {
			break
		}

		for _, anc := range ancs {
			logger.Sugar().Infow("send", "Announcement", anc, "Announcements", len(ancs))
			if err := sendOne(ctx, anc); err != nil {
				logger.Sugar().Errorw("send", "error", err)
				continue
			}
			time.Sleep(100 * time.Millisecond)
		}

		offset += limit
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			send(ctx, basetypes.NotifChannel_ChannelEmail)
			send(ctx, basetypes.NotifChannel_ChannelSMS)
		case <-ctx.Done():
			return
		}
	}
}
