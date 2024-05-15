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
)

type calculateHandler struct {
	req    *eventmwpb.CalcluateEventRewardsRequest
	reward *eventmwpb.Reward
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
	reward, err := eventmwcli.CalcluateEventRewards(ctx, &eventmwpb.CalcluateEventRewardsRequest{
		AppID:       in.AppID,
		UserID:      in.UserID,
		EventType:   in.EventType,
		GoodID:      in.GoodID,
		AppGoodID:   in.AppGoodID,
		Consecutive: in.Consecutive,
		Amount:      in.Amount,
	})
	if err != nil {
		return err
	}
	if reward == nil || reward.TaskID == "" {
		return fmt.Errorf("miss reward")
	}
	handler := &calculateHandler{
		req:    in,
		reward: reward,
	}

	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		req := &eventmwpb.RewardReliableRequest{
			AppID:       in.AppID,
			UserID:      in.UserID,
			TaskID:      reward.TaskID,
			EventID:     ev.EntID,
			Credits:     reward.Credits,
			CoinRewards: reward.CoinRewards,
		}
		return publisher.Update(
			basetypes.MsgID_ReliableEventRewardReq.String(),
			nil,
			nil,
			nil,
			req,
		)
	}); err != nil {
		logger.Sugar().Errorw(
			"ReliableEventReward",
			"AppID", handler.req.AppID,
			"UserID", handler.req.UserID,
			"EventType", handler.req.EventType,
			"Error", err,
		)
	}

	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		req := &eventmwpb.RewardUnReliableRequest{
			AppID:   in.AppID,
			UserID:  in.UserID,
			TaskID:  reward.TaskID,
			EventID: ev.EntID,
			Coupons: reward.CouponRewards,
		}
		return publisher.Update(
			basetypes.MsgID_UnReliableEventRewardReq.String(),
			nil,
			nil,
			nil,
			req,
		)
	}); err != nil {
		logger.Sugar().Errorw(
			"UnReliableEventReward",
			"AppID", handler.req.AppID,
			"UserID", handler.req.UserID,
			"EventType", handler.req.EventType,
			"Error", err,
		)
	}

	return nil
}
