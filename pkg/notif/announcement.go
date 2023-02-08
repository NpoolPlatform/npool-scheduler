package notif

import (
	"context"
	"fmt"
	notifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif"
	"math"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	usercli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	userpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	channelpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/channel"

	announcementpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	announcementcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement"

	sendstatemgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/announcement/sendstate"
	sendstatecli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement/sendstate"

	thirdpb "github.com/NpoolPlatform/message/npool/third/mgr/v1/template/notif"
	thirdcli "github.com/NpoolPlatform/third-middleware/pkg/client/template/notif"
)

const SendLimit = 50

func sendAnnouncement(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)
	endAt := uint32(time.Now().Unix())
	channel := channelpb.NotifChannel_ChannelEmail

	for {
		aInfos, _, err := announcementcli.GetAnnouncements(ctx, &announcementpb.Conds{
			EndAt: &commonpb.Uint32Val{
				Op:    cruder.GT,
				Value: endAt,
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "offset", offset, "limit", limit, "error", err)
			return
		}
		if len(aInfos) == 0 {
			return
		}

		for _, val := range aInfos {
			uOffset := int32(0)
			uLimit := int32(1000)
			userInfos, _, err := usercli.GetUsers(ctx, nil, uOffset, uLimit)
			if err != nil {
				logger.Sugar().Errorw("sendNotif", "offset", uOffset, "limit", uLimit, "error", err)
				return
			}

			userMap := map[string]*userpb.User{}
			userIDs := []string{}
			for _, user := range userInfos {
				userIDs = append(userIDs, user.ID)
				userMap[user.ID] = user
			}

			templateInfo, err := thirdcli.GetNotifTemplateOnly(ctx, &thirdpb.Conds{
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
				logger.Sugar().Errorw("sendNotif", "error", err)
				return
			}

			if templateInfo == nil {
				logger.Sugar().Errorw("sendNotif", "error", "template is empty")
				return
			}
			sendAnnou, _, err := announcementcli.GetAnnouncements(ctx, &announcementpb.Conds{
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
				EndAt: &commonpb.Uint32Val{
					Op:    cruder.GT,
					Value: endAt,
				},
				UserIDs: &commonpb.StringSliceVal{
					Op:    cruder.IN,
					Value: userIDs,
				},
			}, 0, int32(len(userIDs)))
			if err != nil {
				logger.Sugar().Errorw("sendNotif", "offset", offset, "limit", limit, "error", err)
				return
			}

			sOffset := 0
			toUserEmails := []*string{}
			for {
				// 切片分页每次取50条
				start, end := slicePage(sOffset, SendLimit, len(sendAnnou))
				if len(sendAnnou[start:end]) == 0 {
					break
				}

				sendUserIDs := []string{}
				for _, s := range sendAnnou[start:end] {
					//拼接已发送用户
					_, ok := userMap[s.UserID]
					if !ok {
						continue
					}
					sendUserIDs = append(sendUserIDs, s.UserID)
				}

				fmt.Println()
				sendInfos := []*sendstatemgrpb.SendStateReq{}

				for _, userID := range userIDs {
					isSend := false
					for _, sendUserID := range sendUserIDs {
						if userID == sendUserID {
							isSend = true
						}
					}
					//拼接未发送用户
					if !isSend {
						user, ok := userMap[userID]
						if ok {
							toUserEmails = append(toUserEmails, &user.EmailAddress)
							sendInfos = append(sendInfos, &sendstatemgrpb.SendStateReq{
								AppID:          &val.AppID,
								UserID:         &userID,
								AnnouncementID: &val.AnnouncementID,
								Channel:        &channel,
							})
						}
					}
				}

				sOffset += SendLimit

				fmt.Println(templateInfo)
				//err = verifyemail.SendEmailsByAWS(val.Title, val.Content, templateInfo.Sender, toUserEmails)
				//if err != nil {
				//	logger.Sugar().Errorw("sendNotif", "error", err.Error())
				//	continue
				//}
				fmt.Println(sendInfos)
				err = sendstatecli.CreateSendStates(ctx, sendInfos)
				if err != nil {
					logger.Sugar().Errorw("sendNotif", "error", err.Error())
					return
				}
			}

		}
	}
}

func slicePage(page, pageSize, nums int) (sliceStart, sliceEnd int) {
	if pageSize > nums {
		return 0, nums
	}

	pageCount := int(math.Ceil(float64(nums) / float64(pageSize)))
	if page > pageCount {
		return 0, 0
	}
	sliceStart = (page - 1) * pageSize
	sliceEnd = sliceStart + pageSize

	if sliceEnd > nums {
		sliceEnd = nums
	}
	return sliceStart, sliceEnd
}
