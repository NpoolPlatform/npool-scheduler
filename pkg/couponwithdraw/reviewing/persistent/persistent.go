package persistent

import (
	"context"
	"fmt"

	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	reviewtypes "github.com/NpoolPlatform/message/npool/basetypes/review/v1"
	allocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coupon/allocated"
	reviewmwcli "github.com/NpoolPlatform/review-middleware/pkg/client/review"

	allocatedmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/coupon/allocated"
	couponwithdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw/coupon"
	couponwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw/coupon"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/couponwithdraw/reviewing/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, couponwithdraw interface{}, reward, notif, done chan interface{}) error {
	_couponwithdraw, ok := couponwithdraw.(*types.PersistentCouponWithdraw)
	if !ok {
		return fmt.Errorf("invalid coupon withdraw")
	}

	defer asyncfeed.AsyncFeed(ctx, _couponwithdraw, done)

	review, err := reviewmwcli.GetReview(ctx, _couponwithdraw.ReviewID)
	if err != nil {
		return err
	}
	if review == nil {
		return fmt.Errorf("review not found")
	}

	state := ledgertypes.WithdrawState_DefaultWithdrawState
	switch review.State {
	case reviewtypes.ReviewState_Rejected:
		state = ledgertypes.WithdrawState_Rejected
	case reviewtypes.ReviewState_Approved:
		state = ledgertypes.WithdrawState_Approved
	default:
		return nil
	}
	if _, err := couponwithdrawmwcli.UpdateCouponWithdraw(ctx, &couponwithdrawmwpb.CouponWithdrawReq{
		ID:    &_couponwithdraw.ID,
		State: &state,
	}); err != nil {
		return err
	}

	if state != ledgertypes.WithdrawState_Approved {
		return nil
	}
	coupon, err := allocatedmwcli.GetCoupon(ctx, _couponwithdraw.AllocatedID)
	if err != nil {
		return err
	}
	if coupon == nil {
		return fmt.Errorf("coupon not found")
	}
	if coupon.Used {
		return fmt.Errorf("coupon already used")
	}
	used := true
	if _, err := allocatedmwcli.UpdateCoupon(ctx, &allocatedmwpb.CouponReq{
		ID:            &coupon.ID,
		Used:          &used,
		UsedByOrderID: &_couponwithdraw.EntID,
	}); err != nil {
		return err
	}
	return nil
}
