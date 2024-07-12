package reliable

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	inspiremwsvcname "github.com/NpoolPlatform/inspire-middleware/pkg/servicename"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	coinallocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coin/allocated"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	taskusermwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/user"
	usercredithistorymwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/user/credit/history"
	userrewardmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/user/reward"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
)

type calculateHandler struct {
	req        *eventmwpb.RewardReliableRequest
	taskUserID string
}

func (h *calculateHandler) withCreateCredit(dispose *dtmcli.SagaDispose) {
	for _, credit := range h.req.Credits {
		id := uuid.NewString()
		req := &usercredithistorymwpb.UserCreditHistoryReq{
			EntID:   &id,
			AppID:   &credit.AppID,
			UserID:  &credit.UserID,
			TaskID:  &h.req.TaskID,
			EventID: &h.req.EventID,
			Credits: &credit.Credits,
		}
		rewardReq := &userrewardmwpb.UserRewardReq{
			EntID:         &id,
			AppID:         &h.req.AppID,
			UserID:        &h.req.UserID,
			ActionCredits: &credit.Credits,
		}
		dispose.Add(
			inspiremwsvcname.ServiceDomain,
			"inspire.middleware.user.credit.history.v1.Middleware/CreateUserCreditHistory",
			"inspire.middleware.user.credit.history.v1.Middleware/DeleteUserCreditHistory",
			&usercredithistorymwpb.CreateUserCreditHistoryRequest{
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
}

func (h *calculateHandler) withCreateCoin(dispose *dtmcli.SagaDispose) {
	if h.req.CoinRewards == nil {
		return
	}
	for _, coin := range h.req.CoinRewards {
		id := uuid.NewString()
		req := &coinallocatedmwpb.CoinAllocatedReq{
			EntID:        &id,
			AppID:        &coin.AppID,
			UserID:       &coin.UserID,
			CoinConfigID: &coin.CoinConfigID,
			CoinTypeID:   &coin.CoinTypeID,
			Value:        &coin.CoinRewards,
		}
		dispose.Add(
			inspiremwsvcname.ServiceDomain,
			"inspire.middleware.coin.allocated.v1.Middleware/CreateCoinAllocated",
			"inspire.middleware.coin.allocated.v1.Middleware/DeleteCoinAllocated",
			&coinallocatedmwpb.CreateCoinAllocatedRequest{
				Info: req,
			},
		)
	}
}

func (h *calculateHandler) withCreateLedgerStatements(dispose *dtmcli.SagaDispose, ev *eventmwpb.Event) {
	if h.req.CoinRewards == nil {
		return
	}
	rollback := true
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_SimulateMiningBenefit
	reqs := []*statementmwpb.StatementReq{}
	for _, coin := range h.req.CoinRewards {
		id := uuid.NewString()
		now := uint32(time.Now().Unix())
		ioExtra := fmt.Sprintf(
			`{"EventID":"%v","EventType":"%v","TaskID":"%v","TaskUserID":"%v"}`,
			ev.EntID,
			ev.EventType,
			h.req.TaskID,
			h.taskUserID,
		)
		reqs = append(reqs, &statementmwpb.StatementReq{
			EntID:      &id,
			AppID:      &coin.AppID,
			UserID:     &coin.UserID,
			CoinTypeID: &coin.CoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &coin.CoinRewards,
			IOExtra:    &ioExtra,
			CreatedAt:  &now,
			Rollback:   &rollback,
		})
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v2.Middleware/CreateStatements",
		"ledger.middleware.ledger.statement.v2.Middleware/DeleteStatements",
		&statementmwpb.CreateStatementsRequest{
			Infos: reqs,
		},
	)
}

func (h *calculateHandler) WithCreateTaskUser(dispose *dtmcli.SagaDispose) {
	taskState := inspiretypes.TaskState_Done
	rewardState := inspiretypes.RewardState_Issued
	rewardInfo := ""
	req := &taskusermwpb.TaskUserReq{
		EntID:       &h.taskUserID,
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
	req := eventmwpb.RewardReliableRequest{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*eventmwpb.RewardReliableRequest)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	ev, err := eventmwcli.GetEvent(ctx, in.EventID)
	if err != nil {
		return err
	}

	handler := &calculateHandler{
		req: in,
	}
	id := uuid.NewString()
	handler.taskUserID = id

	const timeoutSeconds = 30
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
	})
	// create credit
	handler.withCreateCredit(sagaDispose)

	// create coins
	handler.withCreateCoin(sagaDispose)

	// // create ledger
	handler.withCreateLedgerStatements(sagaDispose, ev)

	// // create task user
	handler.WithCreateTaskUser(sagaDispose)

	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
