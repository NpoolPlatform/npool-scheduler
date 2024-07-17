package credit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	inspiremwsvcname "github.com/NpoolPlatform/inspire-middleware/pkg/servicename"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	creditallocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/credit/allocated"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	taskusermwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/user"
	userrewardmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/user/reward"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
)

type calculateHandler struct {
	req *eventmwpb.CreditRewardRequest
}

func (h *calculateHandler) retryReward() {
	if h.req.RetryCount == 0 {
		return
	}
	time.Sleep(1 * time.Second)
	req := h.req
	req.RetryCount--
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
			"Error", err,
		)
	}
}

func (h *calculateHandler) withCreateCredit(dispose *dtmcli.SagaDispose) {
	id := uuid.NewString()
	extra := fmt.Sprintf(
		`{"TaskUserID":"%v"}`,
		h.req.TaskUserID,
	)
	req := &creditallocatedmwpb.CreditAllocatedReq{
		EntID:  &id,
		AppID:  &h.req.AppID,
		UserID: &h.req.UserID,
		Value:  &h.req.Credits,
		Extra:  &extra,
	}
	rewardReq := &userrewardmwpb.UserRewardReq{
		EntID:         &id,
		AppID:         &h.req.AppID,
		UserID:        &h.req.UserID,
		ActionCredits: &h.req.Credits,
	}
	dispose.Add(
		inspiremwsvcname.ServiceDomain,
		"inspire.middleware.credit.allocated.v1.Middleware/CreateCreditAllocated",
		"inspire.middleware.credit.allocated.v1.Middleware/DeleteCreditAllocated",
		&creditallocatedmwpb.CreateCreditAllocatedRequest{
			Info: req,
		},
	)
	dispose.Add(
		inspiremwsvcname.ServiceDomain,
		"inspire.middleware.user.reward.v1.Middleware/AddUserReward",
		"inspire.middleware.user.reward.v1.Middleware/SubUserReward",
		&userrewardmwpb.AddUserRewardRequest{
			Info: rewardReq,
		},
	)
}

func (h *calculateHandler) WithCreateTaskUser(dispose *dtmcli.SagaDispose) {
	taskState := inspiretypes.TaskState_Done
	rewardState := inspiretypes.RewardState_Issued
	rewardInfo := ""
	req := &taskusermwpb.TaskUserReq{
		EntID:       &h.req.TaskUserID,
		AppID:       &h.req.AppID,
		UserID:      &h.req.UserID,
		TaskID:      &h.req.TaskID,
		EventID:     &h.req.EventID,
		TaskState:   &taskState,
		RewardState: &rewardState,
		RewardInfo:  &rewardInfo,
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

func Prepare(body string) (interface{}, error) {
	req := eventmwpb.CreditRewardRequest{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*eventmwpb.CreditRewardRequest)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	handler := &calculateHandler{
		req: in,
	}

	const timeoutSeconds = 30
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
	})
	// create credit
	handler.withCreateCredit(sagaDispose)

	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		handler.retryReward()
		return err
	}

	return nil
}
