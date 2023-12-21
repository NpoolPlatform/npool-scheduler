package persistent

import (
	"context"
	"fmt"

	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	allocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coupon/allocated"

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

func (p *handler) Update(ctx context.Context, couponwithdraw interface{}, notif, done chan interface{}) error {
	_couponwithdraw, ok := couponwithdraw.(*types.PersistentCouponWithdraw)
	if !ok {
		return fmt.Errorf("invalid coupon withdraw")
	}

	defer asyncfeed.AsyncFeed(ctx, _couponwithdraw, done)

	approved := ledgertypes.WithdrawState_Approved
	if _, err := couponwithdrawmwcli.UpdateCouponWithdraw(ctx, &couponwithdrawmwpb.CouponWithdrawReq{
		ID:    &_couponwithdraw.ID,
		State: &approved,
	}); err != nil {
		return err
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
