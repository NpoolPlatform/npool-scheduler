package sentinel

import (
	"context"

	couponwithdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw/coupon"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	couponwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw/coupon"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/couponwithdraw/reviewing/types"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	for {
		couponwithdraws, _, err := couponwithdrawmwcli.GetCouponWithdraws(ctx, &couponwithdrawmwpb.Conds{
			State: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ledgertypes.WithdrawState_Reviewing)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(couponwithdraws) == 0 {
			return nil
		}
		for _, cw := range couponwithdraws {
			cancelablefeed.CancelableFeed(ctx, cw, exec)
		}
		offset += limit
	}
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	if couponwithdraw, ok := ent.(*types.PersistentCouponWithdraw); ok {
		return couponwithdraw.EntID
	}
	return ent.(*couponwithdrawmwpb.CouponWithdraw).EntID
}
