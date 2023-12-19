package types

import (
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	reviewtypes "github.com/NpoolPlatform/message/npool/basetypes/review/v1"
	couponwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw/coupon"
)

type PersistentCouponWithdraw struct {
	*couponwithdrawmwpb.CouponWithdraw
	NewWithdrawState ledgertypes.WithdrawState
	NewReviewState   reviewtypes.ReviewState
	NeedUpdateReview bool
	Error            error
}
