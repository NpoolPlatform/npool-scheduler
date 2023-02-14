//nolint:dupl
package notif

import (
	"context"
	"fmt"
	"strings"
	"time"

	usercli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	g11ncli "github.com/NpoolPlatform/g11n-middleware/pkg/client/applang"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
	userpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	g11npb "github.com/NpoolPlatform/message/npool/g11n/mgr/v1/applang"
	channelpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/channel"
	notifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif"
	"github.com/NpoolPlatform/message/npool/third/mgr/v1/usedfor"
	notifcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	thirdcli "github.com/NpoolPlatform/third-middleware/pkg/client/notif"
	thirdpkg "github.com/NpoolPlatform/third-middleware/pkg/template"

	notifchannelpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif/notifchannel"
	notifchannelcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/notifchannel"

	frontendmgrpb "github.com/NpoolPlatform/message/npool/third/mgr/v1/template/frontend"
	frontendcli "github.com/NpoolPlatform/third-middleware/pkg/client/template/frontend"

	emailmgrpb "github.com/NpoolPlatform/message/npool/third/mgr/v1/template/email"
	emailcli "github.com/NpoolPlatform/third-middleware/pkg/client/template/email"

	smsmgrpb "github.com/NpoolPlatform/message/npool/third/mgr/v1/template/sms"
	smscli "github.com/NpoolPlatform/third-middleware/pkg/client/template/sms"
)

var (
	date  = time.Now().Format("2006-01-02")
	time1 = time.Now().Format("15:04:05")
)

func CreateNotif(
	ctx context.Context,
	appID, userID, extra string,
	amount, coinUnit, address *string,
	eventType usedfor.UsedFor,
) {
	channelInfos, _, err := notifchannelcli.GetNotifChannels(ctx, &notifchannelpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: appID,
		},
		EventType: &commonpb.Uint32Val{
			Op:    cruder.EQ,
			Value: uint32(eventType),
		},
	}, 0, int32(len(channelpb.NotifChannel_value)))
	if err != nil {
		logger.Sugar().Errorw("CreateNotif", "error", err.Error())
		return
	}
	notifReq := []*notifmgrpb.NotifReq{}

	for _, val := range channelInfos {
		if val.Channel == channelpb.NotifChannel_ChannelFrontend {
			notifReq = append(
				notifReq,
				createFrontendNotif(ctx, appID, userID, amount, coinUnit, address, eventType)...,
			)
		}
		if val.Channel == channelpb.NotifChannel_ChannelEmail {
			email := createEmailNotif(ctx, appID, userID, amount, coinUnit, address, eventType)
			if email != nil {
				notifReq = append(notifReq, email)
			}
		}
		if val.Channel == channelpb.NotifChannel_ChannelSMS {
			sms := createSMSNotif(ctx, appID, userID, amount, coinUnit, address, eventType)
			if sms != nil {
				notifReq = append(notifReq, sms)
			}
		}
	}
	for key := range notifReq {
		notifReq[key].Extra = &extra
	}
	_, err = notifcli.CreateNotifs(ctx, notifReq)
	if err != nil {
		logger.Sugar().Errorw("CreateNotif", "error", err.Error())
		return
	}
}

func createFrontendNotif(
	ctx context.Context,
	appID, userID string,
	amount, coinUnit, address *string,
	eventType usedfor.UsedFor,
) []*notifmgrpb.NotifReq {
	offset := uint32(0)
	limit := uint32(1000)
	notifChannel := channelpb.NotifChannel_ChannelFrontend
	notifReq := []*notifmgrpb.NotifReq{}
	for {
		templateInfos, _, err := frontendcli.GetFrontendTemplates(ctx, &frontendmgrpb.Conds{
			AppID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: appID,
			},
			UsedFor: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(eventType.Number()),
			},
		}, offset, limit)
		offset += limit
		if err != nil {
			logger.Sugar().Errorw("CreateNotif", "error", err.Error())
			return nil
		}
		if len(templateInfos) == 0 {
			break
		}
		useTemplate := true

		for _, val := range templateInfos {
			content := thirdpkg.ReplaceVariable(
				val.Content,
				nil,
				nil,
				amount,
				coinUnit,
				&date,
				&time1,
				address,
			)

			notifReq = append(notifReq, &notifmgrpb.NotifReq{
				AppID:       &appID,
				UserID:      &userID,
				LangID:      &val.LangID,
				EventType:   &eventType,
				UseTemplate: &useTemplate,
				Title:       &val.Title,
				Content:     &content,
				Channel:     &notifChannel,
			})
		}
	}
	return notifReq
}

func createEmailNotif(
	ctx context.Context,
	appID, userID string,
	amount, coinUnit, address *string,
	eventType usedfor.UsedFor,
) *notifmgrpb.NotifReq {
	notifChannel := channelpb.NotifChannel_ChannelEmail

	mainLang, err := g11ncli.GetLangOnly(ctx, &g11npb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: appID,
		},
		Main: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
	})
	if err != nil {
		logger.Sugar().Errorw("sendNotif", "error", err)
		return nil
	}
	if mainLang == nil {
		logger.Sugar().Errorw(
			"sendNotif",
			"AppID", appID,
			"error", "MainLang is invalid")
		return nil
	}
	templateInfo, err := emailcli.GetEmailTemplateOnly(ctx, &emailmgrpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: appID,
		},
		UsedFor: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(eventType.Number()),
		},
		LangID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: mainLang.GetLangID(),
		},
	})
	if err != nil {
		logger.Sugar().Errorw("CreateNotif", "error", err.Error())
		return nil
	}

	if templateInfo == nil {
		logger.Sugar().Errorw(
			"CreateNotif",
			"AppID",
			appID,
			"UsedFor",
			eventType.String(),
			"LangID",
			mainLang.LangID,
			"error",
			"template not exists",
		)
		return nil
	}

	useTemplate := true
	content := thirdpkg.ReplaceVariable(
		templateInfo.Body,
		nil,
		nil,
		amount,
		coinUnit,
		&date,
		&time1,
		address,
	)

	return &notifmgrpb.NotifReq{
		AppID:       &appID,
		UserID:      &userID,
		LangID:      &templateInfo.LangID,
		EventType:   &eventType,
		UseTemplate: &useTemplate,
		Title:       &templateInfo.Subject,
		Content:     &content,
		Channel:     &notifChannel,
	}
}

func createSMSNotif(
	ctx context.Context,
	appID, userID string,
	amount, coinUnit, address *string,
	eventType usedfor.UsedFor,
) *notifmgrpb.NotifReq {
	notifChannel := channelpb.NotifChannel_ChannelSMS
	mainLang, err := g11ncli.GetLangOnly(ctx, &g11npb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: appID,
		},
		Main: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
	})
	if err != nil {
		logger.Sugar().Errorw("sendNotif", "error", err)
		return nil
	}
	if mainLang == nil {
		logger.Sugar().Errorw(
			"sendNotif",
			"AppID", appID,
			"error", "MainLang is invalid")
		return nil
	}
	templateInfo, err := smscli.GetSMSTemplateOnly(ctx, &smsmgrpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: appID,
		},
		UsedFor: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(eventType.Number()),
		},
		LangID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: mainLang.GetLangID(),
		},
	})
	if err != nil {
		logger.Sugar().Errorw("CreateNotif", "error", err.Error())
		return nil
	}

	if templateInfo == nil {
		logger.Sugar().Errorw(
			"CreateNotif",
			"AppID",
			appID,
			"UsedFor",
			eventType.String(),
			"LangID",
			mainLang.LangID,
			"error",
			"template not exists",
		)
		return nil
	}

	useTemplate := true
	content := thirdpkg.ReplaceVariable(
		templateInfo.Message,
		nil,
		nil,
		amount,
		coinUnit,
		&date,
		&time1,
		address,
	)

	return &notifmgrpb.NotifReq{
		AppID:       &appID,
		UserID:      &userID,
		LangID:      &templateInfo.LangID,
		EventType:   &eventType,
		UseTemplate: &useTemplate,
		Title:       &templateInfo.Subject,
		Content:     &content,
		Channel:     &notifChannel,
	}
}

//nolint:gocognit
func sendNotifEmail(ctx context.Context) {
	offset := int32(0)
	limit := int32(5)
	for {
		notifs, _, err := notifcli.GetNotifs(ctx, &notifmgrpb.Conds{
			Channel: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(channelpb.NotifChannel_ChannelEmail),
			},
			Notified: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: false,
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "offset", offset, "limit", limit, "error", err)
			return
		}

		logger.Sugar().Infow("sendNotif", "Notifs", len(notifs), "Offset", offset, "Limit", limit)

		offset += limit
		if len(notifs) == 0 {
			return
		}

		appIDs := []string{}
		langIDs := []string{}
		usedFors := []string{}

		for _, val := range notifs {
			mainLang, err := g11ncli.GetLangOnly(ctx, &g11npb.Conds{
				AppID: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: val.AppID,
				},
				Main: &commonpb.BoolVal{
					Op:    cruder.EQ,
					Value: true,
				},
			})
			if err != nil {
				logger.Sugar().Errorw("sendNotif", "error", err)
				continue
			}
			if mainLang == nil {
				logger.Sugar().Errorw(
					"sendNotif",
					"AppID", val.AppID,
					"error", "MainLang is invalid")
				continue
			}

			appIDs = append(appIDs, val.AppID)
			langIDs = append(appIDs, mainLang.LangID) //nolint
			usedFors = append(usedFors, val.EventType.String())
		}
		templateInfos, _, err := emailcli.GetEmailTemplates(ctx, &emailmgrpb.Conds{
			AppIDs: &commonpb.StringSliceVal{
				Op:    cruder.IN,
				Value: appIDs,
			},
			LangIDs: &commonpb.StringSliceVal{
				Op:    cruder.IN,
				Value: langIDs,
			},
			UsedFors: &commonpb.StringSliceVal{
				Op:    cruder.IN,
				Value: usedFors,
			},
		}, 0, uint32(len(appIDs)))
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "error", err)
			continue
		}

		if len(templateInfos) == 0 {
			logger.Sugar().Errorw("sendNotif", "error", "template not exist")
			continue
		}

		templateMap := map[string]*emailmgrpb.EmailTemplate{}

		for _, val := range templateInfos {
			templateMap[fmt.Sprintf("%v_%v_%v", val.AppID, val.LangID, val.UsedFor)] = val
		}

		userIDs := []string{}
		for _, val := range notifs {
			userIDs = append(userIDs, val.UserID)
		}
		userInfos, _, err := usercli.GetManyUsers(ctx, userIDs)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "error", err)
			continue
		}

		userMap := map[string]*userpb.User{}
		for _, val := range userInfos {
			userMap[val.ID] = val
		}

		ids := []string{}
		for _, val := range notifs {
			user, ok := userMap[val.UserID]
			if !ok {
				logger.Sugar().Errorw("sendNotif", "error", "user is invalid")
				continue
			}
			if !strings.Contains(user.EmailAddress, "@") {
				logger.Sugar().Errorw("sendNotif", "userID", val.UserID)
				continue
			}

			template, ok := templateMap[fmt.Sprintf("%v_%v_%v", val.AppID, val.LangID, val.EventType)]
			if !ok {
				logger.Sugar().Errorw(
					"sendNotif",
					"AppID", val.AppID,
					"MainLangID", val.LangID,
					"EventType", val.EventType,
					"Error", "template is invalid or not main lang",
				)
				continue
			}

			logger.Sugar().Infow(
				"sendNotif",
				"Title", val.Title,
				"Content", val.Content,
				"Sender", template.Sender,
				"EmailAddress", user.EmailAddress,
			)
			err = thirdcli.SendNotifEmail(ctx, val.Title, val.Content, template.Sender, user.EmailAddress)
			if err != nil {
				logger.Sugar().Errorw("sendNotif", "error", err.Error())
				continue
			}
			ids = append(ids, val.ID)
		}

		if len(ids) == 0 {
			continue
		}

		_, err = notifcli.UpdateNotifs(ctx, ids, true)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "error", err.Error())
			continue
		}
	}
}
