package calculate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	"github.com/google/uuid"
)

type calculateHandler struct {
	req *eventmwpb.CalcluateEventRewardsRequest
}

func Prepare(body string) (interface{}, error) {
	req := eventmwpb.CalcluateEventRewardsRequest{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*eventmwpb.CalcluateEventRewardsRequest)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	ev, err := eventmwcli.GetEventOnly(ctx, &eventmwpb.Conds{
		AppID:     &basetypes.StringVal{Op: cruder.EQ, Value: in.AppID},
		EventType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(in.EventType)},
	})
	if err != nil {
		return err
	}

	// calculate reward
	rewards, err := eventmwcli.CalcluateEventRewards(ctx, &eventmwpb.CalcluateEventRewardsRequest{
		AppID:       in.AppID,
		UserID:      in.UserID,
		EventType:   in.EventType,
		GoodID:      in.GoodID,
		AppGoodID:   in.AppGoodID,
		Consecutive: in.Consecutive,
		Amount:      in.Amount,
	})
	fmt.Printf("AppID: %v, UserID: %v, EventType: %v, GoodID: %v, AppGoodID: %v, Consecutive: %v, Amount: %v;\n",
		in.AppID, in.UserID, in.EventType, in.GoodID, in.AppGoodID, in.Consecutive, in.Amount)
	if err != nil {
		return err
	}
	if len(rewards) == 0 {
		return fmt.Errorf("miss reward")
	}

	handler := &calculateHandler{
		req: in,
	}

	for _, reward := range rewards {
		if reward == nil || reward.TaskID == "" {
			return fmt.Errorf("miss reward")
		}
		taskUserID := uuid.NewString()
		if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
			req := &eventmwpb.CreditRewardRequest{
				AppID:      in.AppID,
				UserID:     reward.UserID,
				TaskID:     reward.TaskID,
				TaskUserID: taskUserID,
				EventID:    ev.EntID,
				Credits:    reward.Credits,
				RetryCount: 1,
			}
			return publisher.Update(
				basetypes.MsgID_EventRewardCreditReq.String(),
				nil,
				nil,
				nil,
				req,
			)
		}); err != nil {
			logger.Sugar().Errorw(
				"EventRewardCredit",
				"AppID", handler.req.AppID,
				"UserID", handler.req.UserID,
				"RewardUserID", reward.UserID,
				"EventType", handler.req.EventType,
				"Error", err,
			)
		}
		if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
			req := &eventmwpb.CoinRewardRequest{
				AppID:       in.AppID,
				UserID:      reward.UserID,
				TaskID:      reward.TaskID,
				TaskUserID:  taskUserID,
				EventID:     ev.EntID,
				CoinRewards: reward.CoinRewards,
			}
			return publisher.Update(
				basetypes.MsgID_EventRewardCoinReq.String(),
				nil,
				nil,
				nil,
				req,
			)
		}); err != nil {
			logger.Sugar().Errorw(
				"EventRewardCoin",
				"AppID", handler.req.AppID,
				"UserID", handler.req.UserID,
				"RewardUserID", reward.UserID,
				"EventType", handler.req.EventType,
				"Error", err,
			)
		}

		if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
			req := &eventmwpb.CouponRewardRequest{
				AppID:      in.AppID,
				UserID:     reward.UserID,
				TaskID:     reward.TaskID,
				TaskUserID: taskUserID,
				EventID:    ev.EntID,
				Coupons:    reward.CouponRewards,
			}
			return publisher.Update(
				basetypes.MsgID_EventRewardCouponReq.String(),
				nil,
				nil,
				nil,
				req,
			)
		}); err != nil {
			logger.Sugar().Errorw(
				"EventRewardCoupon",
				"AppID", handler.req.AppID,
				"UserID", handler.req.UserID,
				"RewardUserID", reward.UserID,
				"EventType", handler.req.EventType,
				"Error", err,
			)
		}
	}

	return nil
}
