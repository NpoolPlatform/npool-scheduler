package notification

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	applangmwpb "github.com/NpoolPlatform/message/npool/g11n/mw/v1/applang"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	emailtmplmgrpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/email"
	smstmplmgrpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/sms"
	sendmwpb "github.com/NpoolPlatform/message/npool/third/mw/v1/send"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	applangmwcli "github.com/NpoolPlatform/g11n-middleware/pkg/client/applang"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	emailtmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/email"
	smstmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/sms"
	sendmwcli "github.com/NpoolPlatform/third-middleware/pkg/client/send"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	"github.com/google/uuid"
)

//nolint
func sendOne(ctx context.Context, notif *notifmwpb.Notif) error {
	user, err := usermwcli.GetUser(ctx, notif.AppID, notif.UserID)
	if err != nil {
		return err
	}
	if user == nil {
		return fmt.Errorf("user invalid")
	}

	langConds := &applangmwpb.Conds{
		AppID: &basetypes.StringVal{Op: cruder.EQ, Value: notif.AppID},
	}
	if user.SelectedLangID != nil {
		langConds.LangID = &basetypes.StringVal{Op: cruder.EQ, Value: *user.SelectedLangID}
	} else {
		langConds.Main = &basetypes.BoolVal{Op: cruder.EQ, Value: true}
	}

	lang, err := applangmwcli.GetLangOnly(ctx, langConds)
	if err != nil {
		return err
	}
	if lang == nil {
		return fmt.Errorf("app %v main lang invalid", notif.AppID)
	}

	if notif.LangID != lang.LangID {
		return nil
	}

	logger.Sugar().Infow(
		"sendOne",
		"AppID", user.AppID,
		"UserID", user.ID,
		"EmailAddress", user.EmailAddress,
		"ID", notif.ID,
		"EventType", notif.EventType,
		"State", "Sending")
	req := &sendmwpb.SendMessageRequest{
		Subject: notif.Title,
		Content: notif.Content,
	}

	switch notif.Channel {
	case basetypes.NotifChannel_ChannelEmail:
		tmpl, err := emailtmplmwcli.GetEmailTemplateOnly(ctx, &emailtmplmgrpb.Conds{
			AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: notif.AppID},
			LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: notif.LangID},
			UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(notif.EventType)},
		})
		if err != nil {
			return err
		}
		if tmpl == nil {
			return fmt.Errorf("app %v lang %v email template %v not exist", notif.AppID, notif.LangID, notif.EventType)
		}

		req.From = tmpl.Sender
		req.To = user.EmailAddress
		req.ToCCs = tmpl.CCTos
		req.ReplyTos = tmpl.ReplyTos
		req.AccountType = basetypes.SignMethod_Email
	case basetypes.NotifChannel_ChannelSMS:
		tmpl, err := smstmplmwcli.GetSMSTemplateOnly(ctx, &smstmplmgrpb.Conds{
			AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: notif.AppID},
			LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: notif.LangID},
			UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(notif.EventType)},
		})
		if err != nil {
			return err
		}
		if tmpl == nil {
			return fmt.Errorf("app %v lang %v sms template %v not exist", notif.AppID, notif.LangID, notif.EventType)
		}

		req.To = user.PhoneNO
		req.AccountType = basetypes.SignMethod_Mobile
	default:
		return nil
	}

	err = sendmwcli.SendMessage(ctx, req)
	if err != nil {
		logger.Sugar().Infow(
			"sendOne",
			"AppID", user.AppID,
			"UserID", user.ID,
			"EmailAddress", user.EmailAddress,
			"ID", notif.ID,
			"EventType", notif.EventType,
			"Req", req,
			"Error", err,
		)
		return err
	}

	conds := &notifmwpb.Conds{
		Channel: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(notif.Channel)},
	}
	if _, err := uuid.Parse(notif.EventID); err == nil {
		conds.EventID = &basetypes.StringVal{Op: cruder.EQ, Value: notif.EventID}
	}

	notifs, _, err := notifmwcli.GetNotifs(ctx, conds, 0, int32(1000)) //nolint
	if err != nil {
		logger.Sugar().Errorw(
			"sendOne",
			"AppID", user.AppID,
			"UserID", user.ID,
			"EmailAddress", user.EmailAddress,
			"ID", notif.ID,
			"EventType", notif.EventType,
			"EventID", notif.EventID,
			"Error", err,
		)
		return err
	}

	reqs := []*notifmwpb.NotifReq{}
	nofitied := true
	for _, _notif := range notifs {
		reqs = append(reqs, &notifmwpb.NotifReq{
			ID:       &_notif.ID,
			Notified: &nofitied,
		})
	}

	if len(reqs) == 0 {
		logger.Sugar().Errorw(
			"sendOne",
			"AppID", user.AppID,
			"UserID", user.ID,
			"EmailAddress", user.EmailAddress,
			"ID", notif.ID,
			"EventType", notif.EventType,
			"EventID", notif.EventID,
			"Error", "invalid reqs",
		)
		return fmt.Errorf("invalid reqs")
	}

	_, err = notifmwcli.UpdateNotifs(ctx, reqs)
	if err != nil {
		logger.Sugar().Infow(
			"sendOne",
			"AppID", user.AppID,
			"UserID", user.ID,
			"EmailAddress", user.EmailAddress,
			"ID", notif.ID,
			"EventType", notif.EventType,
			"EventID", notif.EventID,
			"Reqs", reqs,
			"Error", err,
		)
		return err
	}

	return nil
}

func send(ctx context.Context, channel basetypes.NotifChannel) {
	offset := int32(0)
	limit := int32(1000)

	for {
		notifs, _, err := notifmwcli.GetNotifs(ctx, &notifmwpb.Conds{
			Notified: &basetypes.BoolVal{Op: cruder.EQ, Value: false},
			Channel:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(channel)},
		}, offset, limit)
		if err != nil {
			return
		}
		if len(notifs) == 0 {
			break
		}

		for _, notif := range notifs {
			if err := sendOne(ctx, notif); err != nil {
				logger.Sugar().Errorw("send", "error", err)
				continue
			}
			time.Sleep(100 * time.Millisecond)
		}

		offset += limit
	}
}
