package notif

import (
	"context"
	"fmt"

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
	thirdtempcli "github.com/NpoolPlatform/third-middleware/pkg/client/template/notif"
)

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
		offset += limit
		if len(notifs) == 0 {
			return
		}

		appIDs := []string{}
		langIDs := []string{}
		usedFors := []string{}

		for _, val := range notifs {
			appIDs = append(appIDs, val.AppID)
			langIDs = append(langIDs, val.LangID)
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
			logger.Sugar().Errorw("sendNotif", "error", "Template not exist")
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
			if user.EmailAddress == "" {
				logger.Sugar().Errorw("sendNotif", "userID", val.UserID, "error", "user EmailAddress is empty")
				continue
			}
			template, ok := templateMap[fmt.Sprintf("%v_%v_%v", val.AppID, val.LangID, val.EventType)]
			if !ok {
				logger.Sugar().Errorw("sendNotif", "error", "template is invalid")
				continue
			}

			logger.Sugar().Infow(
				"sendNotif",
				"Title",
				val.Title,
				"Content",
				val.Content,
				"Sender",
				template.Sender,
				"EmailAddress",
				user.EmailAddress,
			)
			err = thirdcli.SendNotifEmail(ctx, val.Title, val.Content, template.Sender, user.EmailAddress)
			if err != nil {
				logger.Sugar().Errorw("sendNotif", "error", err.Error())
				continue
			}
			ids = append(ids, val.ID)
		}

		send := true
		_, err = notifcli.UpdateNotifs(ctx, ids, &send, nil)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "error", err.Error())
			continue
		}
	}
}
