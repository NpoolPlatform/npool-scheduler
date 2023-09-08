package executor

import (
	"context"
	"fmt"
	"net/mail"
	"regexp"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	applangmwcli "github.com/NpoolPlatform/g11n-middleware/pkg/client/applang"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	usermwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	applangmwpb "github.com/NpoolPlatform/message/npool/g11n/mw/v1/applang"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	emailtmplmgrpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/email"
	smstmplmgrpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template/sms"
	sendmwpb "github.com/NpoolPlatform/message/npool/third/mw/v1/send"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	emailtmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/email"
	smstmplmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/template/sms"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/notification/types"
)

type notifHandler struct {
	*notifmwpb.Notif
	persistent     chan interface{}
	done           chan interface{}
	notifiable     bool
	eventNotifs    []*notifmwpb.Notif
	user           *usermwpb.User
	lang           *applangmwpb.Lang
	messageRequest *sendmwpb.SendMessageRequest
}

func (h *notifHandler) getUser(ctx context.Context) error {
	user, err := usermwcli.GetUser(ctx, h.AppID, h.UserID)
	if err != nil {
		return err
	}
	if user == nil {
		return fmt.Errorf("invalid user")
	}
	h.user = user
	return nil
}

func (h *notifHandler) getLang(ctx context.Context) error {
	conds := &applangmwpb.Conds{
		AppID: &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
	}
	if h.user.SelectedLangID != nil {
		conds.LangID = &basetypes.StringVal{Op: cruder.EQ, Value: *h.user.SelectedLangID}
	} else {
		conds.Main = &basetypes.BoolVal{Op: cruder.EQ, Value: true}
	}
	lang, err := applangmwcli.GetLangOnly(ctx, conds)
	if err != nil {
		return err
	}
	if lang != nil {
		h.lang = lang
		return nil
	}

	conds.Main = &basetypes.BoolVal{Op: cruder.EQ, Value: true}
	lang, err = applangmwcli.GetLangOnly(ctx, conds)
	if err != nil {
		return err
	}
	if lang == nil {
		return fmt.Errorf("invalid main lang")
	}
	h.lang = lang
	return nil
}

func (h *notifHandler) generateEmailMessage(ctx context.Context) error {
	tmpl, err := emailtmplmwcli.GetEmailTemplateOnly(ctx, &emailtmplmgrpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.LangID},
		UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(h.EventType)},
	})
	if err != nil {
		return err
	}
	if tmpl == nil {
		return fmt.Errorf("invalid template")
	}

	if _, err := mail.ParseAddress(h.user.EmailAddress); err != nil {
		h.notifiable = false
		return nil
	}

	h.messageRequest.From = tmpl.Sender
	h.messageRequest.To = h.user.EmailAddress
	h.messageRequest.ToCCs = tmpl.CCTos
	h.messageRequest.ReplyTos = tmpl.ReplyTos
	h.messageRequest.AccountType = basetypes.SignMethod_Email
	return nil
}

func (h *notifHandler) generateSMSMessage(ctx context.Context) error {
	tmpl, err := smstmplmwcli.GetSMSTemplateOnly(ctx, &smstmplmgrpb.Conds{
		AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		LangID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.LangID},
		UsedFor: &basetypes.Int32Val{Op: cruder.EQ, Value: int32(h.EventType)},
	})
	if err != nil {
		return err
	}
	if tmpl == nil {
		return fmt.Errorf("invalid template")
	}

	re := regexp.MustCompile(
		`^(?:(?:\(?(?:00|\+)([1-4]\d\d|[1-9]\d?)\)?)?[` +
			`\-\.\ \\\/]?)?((?:\(?\d{1,}\)?[\-\.\ \\\/]?)` +
			`{0,})(?:[\-\.\ \\\/]?(?:#|ext\.?|extension|x)` +
			`[\-\.\ \\\/]?(\d+))?$`,
	)
	if !re.MatchString(h.user.PhoneNO) {
		h.notifiable = false
		return nil
	}

	h.messageRequest.To = h.user.PhoneNO
	h.messageRequest.AccountType = basetypes.SignMethod_Mobile
	return nil
}

func (h *notifHandler) generateMessageRequest(ctx context.Context) error {
	h.messageRequest = &sendmwpb.SendMessageRequest{
		Subject: h.Title,
		Content: h.Content,
	}
	switch h.Channel {
	case basetypes.NotifChannel_ChannelEmail:
		return h.generateEmailMessage(ctx)
	case basetypes.NotifChannel_ChannelSMS:
		return h.generateSMSMessage(ctx)
	}
	h.notifiable = false
	return nil
}

func (h *notifHandler) getEventNotifs(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		notifs, _, err := notifmwcli.GetNotifs(ctx, &notifmwpb.Conds{
			Channel: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(h.Channel)},
			EventID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EventID},
			UserID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.UserID},
			AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(notifs) == 0 {
			return nil
		}
		h.eventNotifs = notifs
		offset += limit
	}
}

//nolint:gocritic
func (h *notifHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Notif", h.Notif,
			"Notifiable", h.notifiable,
			"Error", *err,
		)
	}

	persistentNotif := &types.PersistentNotif{
		Notif:          h.Notif,
		MessageRequest: h.messageRequest,
		EventNotifs:    h.eventNotifs,
	}
	// TODO: We don't know how to process err here
	if h.notifiable {
		asyncfeed.AsyncFeed(ctx, persistentNotif, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentNotif, h.done)
}

//nolint:gocritic
func (h *notifHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getUser(ctx); err != nil {
		return err
	}
	if err = h.getLang(ctx); err != nil {
		return err
	}
	h.notifiable = h.lang.LangID == h.LangID
	if !h.notifiable {
		return nil
	}
	if err = h.generateMessageRequest(ctx); err != nil {
		return err
	}
	if err = h.getEventNotifs(ctx); err != nil {
		return err
	}

	return nil
}
