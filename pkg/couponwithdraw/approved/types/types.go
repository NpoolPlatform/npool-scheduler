package types

import (
	couponwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw/coupon"
)

type PersistentCouponWithdraw struct {
	*couponwithdrawmwpb.CouponWithdraw
}
