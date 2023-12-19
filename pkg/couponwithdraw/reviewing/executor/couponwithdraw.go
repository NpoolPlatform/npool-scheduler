package executor

import (
	"context"

	couponwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw/coupon"
)

type couponwithdrawHandler struct {
	*couponwithdrawmwpb.CouponWithdraw
	persistent chan interface{}
	notif      chan interface{}
	done       chan interface{}
}

func (h *couponwithdrawHandler) exec(ctx context.Context) error {
	return nil
}
