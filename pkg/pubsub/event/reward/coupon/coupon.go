package coupon

import (
	"context"
	"encoding/json"
	"fmt"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	inspiremwsvcname "github.com/NpoolPlatform/inspire-middleware/pkg/servicename"
	couponallocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coupon/allocated"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	userrewardmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/user/reward"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/shopspring/decimal"

	"github.com/google/uuid"
)

type calculateHandler struct {
	req *eventmwpb.CouponRewardRequest
}

func (h *calculateHandler) withCreateCoupon(dispose *dtmcli.SagaDispose) {
	extra := fmt.Sprintf(
		`{"TaskUserID":"%v"}`,
		h.req.TaskUserID,
	)
	for _, coupon := range h.req.Coupons {
		id := uuid.NewString()
		req := &couponallocatedmwpb.CouponReq{
			EntID:    &id,
			AppID:    &coupon.AppID,
			CouponID: &coupon.CouponID,
			UserID:   &coupon.UserID,
			Cashable: &coupon.Cashable,
			Extra:    &extra,
		}
		dispose.Add(
			inspiremwsvcname.ServiceDomain,
			"inspire.middleware.coupon.allocated.v1.Middleware/CreateDirectCoupon",
			"inspire.middleware.coupon.allocated.v1.Middleware/DeleteCoupon",
			&couponallocatedmwpb.CreateCouponRequest{
				Info: req,
			},
		)
	}
}

func (h *calculateHandler) WithUpdateUserReward(dispose *dtmcli.SagaDispose) {
	id := uuid.NewString()
	for _, coupon := range h.req.Coupons {
		couponCashableAmount := decimal.NewFromInt(0).String()
		if coupon.Cashable {
			couponCashableAmount = coupon.Denomination
		}
		rewardReq := &userrewardmwpb.UserRewardReq{
			EntID:                &id,
			AppID:                &h.req.AppID,
			UserID:               &h.req.UserID,
			CouponAmount:         &coupon.Denomination,
			CouponCashableAmount: &couponCashableAmount,
		}
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

func Prepare(body string) (interface{}, error) {
	req := eventmwpb.CouponRewardRequest{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*eventmwpb.CouponRewardRequest)
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

	// create coupon
	handler.withCreateCoupon(sagaDispose)

	// update user reward
	handler.WithUpdateUserReward(sagaDispose)

	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
