package executor

import (
	"context"
	"fmt"

	couponwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw/coupon"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}
func (e *handler) Exec(ctx context.Context, couponwithdraw interface{}, persistent, notif, done chan interface{}) error {
	_couponwithdraw, ok := couponwithdraw.(*couponwithdrawmwpb.CouponWithdraw)
	if !ok {
		return fmt.Errorf("invalid coupon withdraw")
	}

	h := &couponwithdrawHandler{
		CouponWithdraw: _couponwithdraw,
		persistent:     persistent,
		notif:          notif,
		done:           done,
	}
	return h.exec(ctx)
}
