package event

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	coupon1 "github.com/NpoolPlatform/inspire-middleware/pkg/mw/coupon"
	inspiremwsvcname "github.com/NpoolPlatform/inspire-middleware/pkg/servicename"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinallocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coin/allocated"
	couponmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coupon"
	couponallocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coupon/allocated"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	taskusermwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/task/user"
	usercredithistorymwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/user/credit/history"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
)

type calculateHandler struct {
	req    *eventmwpb.CalcluateEventRewardsRequest
	reward *eventmwpb.Reward
}

func (h *calculateHandler) withCreateCoupon(dispose *dtmcli.SagaDispose, cp *couponmwpb.Coupon) {
	if h.reward == nil {
		return
	}

	id := uuid.NewString()
	req := &couponallocatedmwpb.CouponReq{
		EntID:    &id,
		AppID:    &h.req.AppID,
		CouponID: &cp.EntID,
		UserID:   &h.req.UserID,
	}
	dispose.Add(
		inspiremwsvcname.ServiceDomain,
		"inspire.middleware.coupon.allocated.v1.Middleware/CreateCoupon",
		"inspire.middleware.coupon.allocated.v1.Middleware/DeleteCoupon",
		&couponallocatedmwpb.CreateCouponRequest{
			Info: req,
		},
	)
}

func (h *calculateHandler) withCreateCredit(dispose *dtmcli.SagaDispose, ev *eventmwpb.Event) {
	if h.reward == nil {
		return
	}
	for _, credit := range h.reward.Credits {
		id := uuid.NewString()
		dispose.Add(
			inspiremwsvcname.ServiceDomain,
			"inspire.middleware.user.credit.history.v1.Middleware/CreateUserCreditHistory",
			"inspire.middleware.user.credit.history.v1.Middleware/DeleteUserCreditHistory",
			&usercredithistorymwpb.UserCreditHistoryReq{
				EntID:   &id,
				AppID:   &credit.AppID,
				UserID:  &credit.UserID,
				TaskID:  &h.reward.TaskID,
				EventID: &ev.EntID,
				Credits: &credit.Credits,
			},
		)
	}
}

func (h *calculateHandler) withCreateCoin(dispose *dtmcli.SagaDispose) {
	if h.reward == nil || h.reward.CoinRewards == nil {
		return
	}

	for _, coin := range h.reward.CoinRewards {
		id := uuid.NewString()
		dispose.Add(
			inspiremwsvcname.ServiceDomain,
			"inspire.middleware.coin.allocated.v1.Middleware/CreateCoinAllocated",
			"inspire.middleware.coin.allocated.v1.Middleware/DeleteCoinAllocated",
			&coinallocatedmwpb.CoinAllocatedReq{
				EntID:        &id,
				AppID:        &coin.AppID,
				UserID:       &coin.UserID,
				CoinConfigID: &coin.CoinConfigID,
				CoinTypeID:   &coin.CoinTypeID,
				Value:        &coin.CoinRewards,
			},
		)
	}
}

func (h *calculateHandler) withCreateLedgerStatements(dispose *dtmcli.SagaDispose, ev *eventmwpb.Event) {
	if h.reward == nil || h.reward.CoinRewards == nil {
		return
	}
	rollback := true
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_EventReward
	reqs := []*statementmwpb.StatementReq{}

	for _, coin := range h.reward.CoinRewards {
		id := uuid.NewString()
		now := uint32(time.Now().Unix())
		ioExtra := fmt.Sprintf(
			`{"EventID":"%v","EventType":"%v"}`,
			ev.EntID,
			ev.EventType,
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

func (h *calculateHandler) WithCreateTaskUser(dispose *dtmcli.SagaDispose, ev *eventmwpb.Event) {
	id := uuid.NewString()
	taskState := inspiretypes.TaskState_Done
	rewardState := inspiretypes.RewardState_Issued
	dispose.Add(
		inspiremwsvcname.ServiceDomain,
		"inspire.middleware.task.user.v1.Middleware/CreateTaskUser",
		"inspire.middleware.task.user.v1.Middleware/DeleteTaskUser",
		&taskusermwpb.TaskUserReq{
			EntID:       &id,
			AppID:       &h.req.AppID,
			UserID:      &h.req.UserID,
			TaskID:      &ev.EntID,
			EventID:     &ev.EntID,
			TaskState:   &taskState,
			RewardState: &rewardState,
		},
	)
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
	const timeoutSeconds = 30
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
	})
	coups := []*couponmwpb.Coupon{}
	for _, id := range ev.CouponIDs {
		_id := id
		handler, err := coupon1.NewHandler(
			ctx,
			coupon1.WithEntID(&_id, true),
		)
		if err != nil {
			return err
		}
		_coupon, err := handler.GetCoupon(ctx)
		if err != nil {
			return err
		}
		if _coupon == nil {
			return wlog.Errorf("invalid coupon")
		}

		now := time.Now().Unix()
		if now < int64(_coupon.StartAt) || now > int64(_coupon.EndAt) {
			logger.Sugar().Errorw("coupon can not be issued in current time")
			continue
		}
		coups = append(coups, _coupon)
	}
	for _, coup := range coups {
		// create coupon
		handler.withCreateCoupon(sagaDispose, coup)
	}

	// create credit
	handler.withCreateCredit(sagaDispose, ev)

	// create coins
	handler.withCreateCoin(sagaDispose)

	// create ledger
	handler.withCreateLedgerStatements(sagaDispose, ev)

	// create task user
	handler.WithCreateTaskUser(sagaDispose, ev)

	return nil
}
