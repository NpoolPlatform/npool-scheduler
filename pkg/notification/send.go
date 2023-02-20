package notification

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	applangmgrpb "github.com/NpoolPlatform/message/npool/g11n/mgr/v1/applang"
	chanmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/channel"
	notifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif"
	emailtmplmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/template/email"
	smstmplmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/template/sms"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	sendmwpb "github.com/NpoolPlatform/message/npool/third/mw/v1/send"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	applangmwcli "github.com/NpoolPlatform/g11n-middleware/pkg/client/applang"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	emailtmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/email"
	smstmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/sms"
	sendmwcli "github.com/NpoolPlatform/third-middleware/pkg/client/send"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
)

func sendOne(ctx context.Context, notif *notifmwpb.Notif) error {
	lang, err := applangmwcli.GetLangOnly(ctx, &applangmgrpb.Conds{
		AppID: &commonpb.StringVal{Op: cruder.EQ, Value: notif.AppID},
		Main:  &commonpb.BoolVal{Op: cruder.EQ, Value: true},
	})
	if err != nil {
		return err
	}
	if lang == nil {
		return fmt.Errorf("app %v main lang invalid", notif.AppID)
	}

	if notif.LangID != lang.LangID {
		return nil
	}

	user, err := usermwcli.GetUser(ctx, notif.AppID, notif.UserID)
	if err != nil {
		return err
	}
	if user == nil {
		return fmt.Errorf("user invalid")
	}

	req := &sendmwpb.SendMessageRequest{
		Subject: notif.Title,
		Content: notif.Content,
	}

	switch notif.Channel {
	case chanmgrpb.NotifChannel_ChannelEmail:
		tmpl, err := emailtmplmwcli.GetEmailTemplateOnly(ctx, &emailtmplmgrpb.Conds{
			AppID:   &commonpb.StringVal{Op: cruder.IN, Value: notif.AppID},
			LangID:  &commonpb.StringVal{Op: cruder.IN, Value: notif.LangID},
			UsedFor: &commonpb.Int32Val{Op: cruder.IN, Value: int32(notif.EventType)},
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
	case chanmgrpb.NotifChannel_ChannelSMS:
		tmpl, err := smstmplmwcli.GetSMSTemplateOnly(ctx, &smstmplmgrpb.Conds{
			AppID:   &commonpb.StringVal{Op: cruder.IN, Value: notif.AppID},
			LangID:  &commonpb.StringVal{Op: cruder.IN, Value: notif.LangID},
			UsedFor: &commonpb.Int32Val{Op: cruder.IN, Value: int32(notif.EventType)},
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
		return err
	}

	notified := true
	_, err = notifmwcli.UpdateNotif(ctx, &notifmgrpb.NotifReq{
		ID:       &notif.ID,
		Notified: &notified,
	})
	if err != nil {
		return err
	}

	return nil
}

func send(ctx context.Context, channel chanmgrpb.NotifChannel) {
	offset := int32(0)
	limit := int32(1000)

	for {
		notifs, _, err := notifmwcli.GetNotifs(ctx, &notifmgrpb.Conds{
			Notified: &commonpb.BoolVal{Op: cruder.EQ, Value: false},
			Channel:  &commonpb.Uint32Val{Op: cruder.EQ, Value: uint32(channel)},
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
				return
			}
		}

		offset += limit
	}
}
