package notif

import (
	"context"
	"fmt"
	"strings"
	"time"

	g11ncli "github.com/NpoolPlatform/g11n-middleware/pkg/client/applang"
	g11npb "github.com/NpoolPlatform/message/npool/g11n/mgr/v1/applang"
	thirdpkg "github.com/NpoolPlatform/third-middleware/pkg/template/notif"

	usercli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
	userpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	channelpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/channel"
	notifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif"
	thirdpb "github.com/NpoolPlatform/message/npool/third/mgr/v1/template/notif"
	notifcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	thirdcli "github.com/NpoolPlatform/third-middleware/pkg/client/notif"

	thirdtempmgrpb "github.com/NpoolPlatform/message/npool/third/mgr/v1/template/notif"
	thirdtempcli "github.com/NpoolPlatform/third-middleware/pkg/client/template/notif"
)

func CreateNotif(
	ctx context.Context,
	appID, userID string,
	amount, coinUnit, address *string,
	eventType notifmgrpb.EventType,
	extra string,
) {
	offset := uint32(0)
	limit := uint32(1000)
	for {
		templateInfos, _, err := thirdtempcli.GetNotifTemplates(ctx, &thirdtempmgrpb.Conds{
			AppID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: appID,
			},
			UsedFor: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(eventType.Number()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "error", err.Error())
			return
		}
		offset += limit
		if len(templateInfos) == 0 {
			logger.Sugar().Errorw("sendNotif", "error", "template not exist")
			return
		}

		notifReq := []*notifmgrpb.NotifReq{}
		useTemplate := true
		date := time.Now().Format("2006-01-02")
		time1 := time.Now().Format("15:04:05")

		for _, val := range templateInfos {
			content := thirdpkg.ReplaceVariable(val.Content, nil, nil, amount, coinUnit, &date, &time1, address)
			notifReq = append(notifReq, &notifmgrpb.NotifReq{
				AppID:       &appID,
				UserID:      &userID,
				AlreadyRead: nil,
				LangID:      &val.LangID,
				EventType:   &eventType,
				UseTemplate: &useTemplate,
				Title:       &val.Title,
				Content:     &content,
				Channels:    []channelpb.NotifChannel{channelpb.NotifChannel_ChannelEmail},
				EmailSend:   nil,
				Extra:       &extra,
			})
		}

		_, err = notifcli.CreateNotifs(ctx, notifReq)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "error", err.Error())
			return
		}
	}
}

//nolint:gocognit
func sendNotif(ctx context.Context) {
	offset := int32(0)
	limit := int32(5)
	for {
		notifs, _, err := notifcli.GetNotifs(ctx, &notifmgrpb.Conds{
			Channels: &commonpb.StringSliceVal{
				Op:    cruder.EQ,
				Value: []string{channelpb.NotifChannel_ChannelEmail.String()},
			},
			EmailSend: &commonpb.BoolVal{
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
				logger.Sugar().Errorw("sendNotif", "error", "MainLang is invalid")
				continue
			}

			appIDs = append(appIDs, val.AppID)
			langIDs = append(appIDs, mainLang.LangID)
			usedFors = append(usedFors, val.EventType.String())
		}
		templateInfos, _, err := thirdtempcli.GetNotifTemplates(ctx, &thirdpb.Conds{
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

		templateMap := map[string]*thirdpb.NotifTemplate{}

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

		send := true
		_, err = notifcli.UpdateNotifs(ctx, ids, &send, nil)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "error", err.Error())
			continue
		}
	}
}
