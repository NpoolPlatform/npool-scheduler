package calculate

import (
	"context"
	"encoding/json"
	"fmt"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	inspiremwsvcname "github.com/NpoolPlatform/inspire-middleware/pkg/servicename"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	taskusermwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/user"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type calculateHandler struct {
	req *eventmwpb.CalcluateEventRewardsRequest
}

func (h *calculateHandler) withCreateTaskUser(dispose *dtmcli.SagaDispose, taskUserID, eventID *string, reward *eventmwpb.Reward) {
	taskState := inspiretypes.TaskState_Done
	rewardState := inspiretypes.RewardState_Issued
	req := &taskusermwpb.TaskUserReq{
		EntID:       taskUserID,
		AppID:       &h.req.AppID,
		UserID:      &reward.UserID,
		TaskID:      &reward.TaskID,
		EventID:     eventID,
		TaskState:   &taskState,
		RewardState: &rewardState,
	}
	dispose.Add(
		inspiremwsvcname.ServiceDomain,
		"inspire.middleware.task.user.v1.Middleware/CreateTaskUser",
		"inspire.middleware.task.user.v1.Middleware/DeleteTaskUser",
		&taskusermwpb.CreateTaskUserRequest{
			Info: req,
		},
	)
}

func (h *calculateHandler) rewardCredit(req *eventmwpb.CreditRewardRequest) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
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
			"AppID", h.req.AppID,
			"UserID", h.req.UserID,
			"RewardUserID", req.UserID,
			"EventType", h.req.EventType,
			"Error", err,
		)
	}
}

func (h *calculateHandler) rewardCoupon(req *eventmwpb.CouponRewardRequest) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
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
			"AppID", h.req.AppID,
			"UserID", h.req.UserID,
			"RewardUserID", req.UserID,
			"EventType", h.req.EventType,
			"Error", err,
		)
	}
}

func (h *calculateHandler) rewardCoin(req *eventmwpb.CoinRewardRequest) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
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
			"AppID", h.req.AppID,
			"UserID", h.req.UserID,
			"RewardUserID", req.UserID,
			"EventType", h.req.EventType,
			"Error", err,
		)
	}
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

		// create task user
		const timeoutSeconds = 30
		sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
			WaitResult:     true,
			RequestTimeout: timeoutSeconds,
			TimeoutToFail:  timeoutSeconds,
		})
		handler.withCreateTaskUser(sagaDispose, &taskUserID, &ev.EntID, reward)
		if err := dtm1.Do(ctx, sagaDispose); err != nil {
			return err
		}

		// reward pubsub
		// reward credits
		credits, err := decimal.NewFromString(reward.Credits)
		if err != nil {
			return err
		}
		if credits.Cmp(decimal.NewFromInt(0)) > 0 {
			req := &eventmwpb.CreditRewardRequest{
				AppID:      in.AppID,
				UserID:     reward.UserID,
				TaskID:     reward.TaskID,
				TaskUserID: taskUserID,
				EventID:    ev.EntID,
				Credits:    reward.Credits,
				RetryCount: 1,
			}
			handler.rewardCredit(req)
		}

		// reward coin
		if len(reward.CoinRewards) > 0 {
			req := &eventmwpb.CoinRewardRequest{
				AppID:       in.AppID,
				UserID:      reward.UserID,
				TaskID:      reward.TaskID,
				TaskUserID:  taskUserID,
				EventID:     ev.EntID,
				CoinRewards: reward.CoinRewards,
			}
			handler.rewardCoin(req)
		}

		// reward coupon
		if len(reward.CouponRewards) > 0 {
			req := &eventmwpb.CouponRewardRequest{
				AppID:      in.AppID,
				UserID:     reward.UserID,
				TaskID:     reward.TaskID,
				TaskUserID: taskUserID,
				EventID:    ev.EntID,
				Coupons:    reward.CouponRewards,
			}
			handler.rewardCoupon(req)
		}
	}

	return nil
}
