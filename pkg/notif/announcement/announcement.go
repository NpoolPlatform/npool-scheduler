package announcement

import (
	"context"
	"strings"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	notifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif"

	usercli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	appusermgrpb "github.com/NpoolPlatform/message/npool/appuser/mgr/v2/appuser"
	userpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	channelpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/channel"

	announcemenmgrtpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/announcement"
	announcementpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	announcementcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement"

	sendstatemgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/announcement/sendstate"
	sendstatepb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement/sendstate"
	sendstatecli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement/sendstate"

	thirdpb "github.com/NpoolPlatform/message/npool/third/mgr/v1/template/notif"
	thirdcli "github.com/NpoolPlatform/third-middleware/pkg/client/notif"
	thirdtempcli "github.com/NpoolPlatform/third-middleware/pkg/client/template/notif"
)

//nolint:gocognit
func sendAnnouncement(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)
	endAt := uint32(time.Now().Unix())
	channel := channelpb.NotifChannel_ChannelEmail

	for {
		aInfos, _, err := announcementcli.GetAnnouncements(ctx, &announcemenmgrtpb.Conds{
			EndAt: &commonpb.Uint32Val{
				Op:    cruder.GT,
				Value: endAt,
			},
			Channels: &commonpb.StringSliceVal{
				Op:    cruder.IN,
				Value: []string{channel.String()},
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("sendAnnouncement", "offset", offset, "limit", limit, "error", err)
			return
		}

		offset += limit
		if len(aInfos) == 0 {
			logger.Sugar().Info("There are no announcements to send within the announcement end at")
			return
		}

		for _, val := range aInfos {
			logger.Sugar().Infow("sendAnnouncement", "ID", val.AnnouncementID, "EndAt", val.EndAt, "EndAt", endAt)

			uOffset := int32(0)
			uLimit := int32(50)
			for {
				userInfos, _, err := usercli.GetUsers(ctx, &appusermgrpb.Conds{
					AppID: &commonpb.StringVal{
						Op:    cruder.EQ,
						Value: val.AppID,
					},
				}, uOffset, uLimit)
				if err != nil {
					logger.Sugar().Errorw("sendAnnouncement", "offset", uOffset, "limit", uLimit, "error", err)
					return
				}
				uOffset += uLimit
				if len(userInfos) == 0 {
					break
				}

				userMap := map[string]*userpb.User{}
				userIDs := []string{}
				for _, user := range userInfos {
					userIDs = append(userIDs, user.ID)
					userMap[user.ID] = user
				}

				templateInfo, err := thirdtempcli.GetNotifTemplateOnly(ctx, &thirdpb.Conds{
					AppID: &commonpb.StringVal{
						Op:    cruder.EQ,
						Value: val.AppID,
					},
					UsedFor: &commonpb.Uint32Val{
						Op:    cruder.EQ,
						Value: uint32(notifmgrpb.EventType_Announcement),
					},
				})
				if err != nil {
					logger.Sugar().Errorw("sendAnnouncement", "error", err)
					return
				}

				if templateInfo == nil {
					logger.Sugar().Errorw("sendAnnouncement", "AppID", val.AppID, "error", "template is empty")
					continue
				}

				sendAnnou, _, err := sendstatecli.GetSendStates(ctx, &sendstatepb.Conds{
					AppID: &commonpb.StringVal{
						Op:    cruder.EQ,
						Value: val.AppID,
					},
					AnnouncementID: &commonpb.StringVal{
						Op:    cruder.EQ,
						Value: val.AnnouncementID,
					},
					Channel: &commonpb.Uint32Val{
						Op:    cruder.EQ,
						Value: uint32(channel.Number()),
					},
					UserIDs: &commonpb.StringSliceVal{
						Op:    cruder.IN,
						Value: userIDs,
					},
				}, 0, int32(len(userIDs)))
				if err != nil {
					logger.Sugar().Errorw("sendAnnouncement", "error", err)
					return
				}

				sendAnnouMap := map[string]*announcementpb.Announcement{}
				for _, send := range sendAnnou {
					sendAnnouMap[send.UserID] = val
				}

				sendInfos := []*sendstatemgrpb.SendStateReq{}

				for _, user := range userInfos {
					if !strings.Contains(user.EmailAddress, "@") {
						continue
					}

					if _, ok := sendAnnouMap[user.ID]; ok {
						logger.Sugar().Infow(
							"sendAnnouncement",
							"AppID", user.AppID,
							"UserID", user.ID,
							"EmailAddress", user.EmailAddress,
							"AnnouncementID", val.AnnouncementID,
							"State", "Sent")
						continue
					}

					logger.Sugar().Infow("sendAnnouncement", "EmailAddress", user.EmailAddress, "AnnouncementID", val.AnnouncementID, "State", "Sending")
					err = thirdcli.SendNotifEmail(ctx, val.Title, val.Content, templateInfo.Sender, user.EmailAddress)
					if err != nil {
						logger.Sugar().Errorw("sendAnnouncement", "error", err.Error(), "Sender", templateInfo.Sender, "To", user.EmailAddress)
						return
					}
					sendInfos = append(sendInfos, &sendstatemgrpb.SendStateReq{
						AppID:          &val.AppID,
						UserID:         &user.ID,
						AnnouncementID: &val.AnnouncementID,
						Channel:        &channel,
					})
				}

				if len(sendInfos) == 0 {
					continue
				}
				err = sendstatecli.CreateSendStates(ctx, sendInfos)
				if err != nil {
					logger.Sugar().Errorw("sendAnnouncement", "error", err.Error())
					return
				}
			}
		}
	}
}
