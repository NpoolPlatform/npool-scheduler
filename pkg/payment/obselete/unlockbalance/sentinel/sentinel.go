package sentinel

import (
	"context"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/unlockbalance/types"
	paymentmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/payment"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) scanPayments(ctx context.Context, state ordertypes.PaymentObseleteState, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		payments, _, err := paymentmwcli.GetPayments(ctx, &paymentmwpb.Conds{
			ObseleteState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(payments) == 0 {
			return nil
		}

		for _, payment := range payments {
			cancelablefeed.CancelableFeed(ctx, payment, exec)
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	return h.scanPayments(ctx, ordertypes.PaymentObseleteState_PaymentObseleteUnlockBalance, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanPayments(ctx, ordertypes.PaymentObseleteState_PaymentObseleteUnlockBalance, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	if payment, ok := ent.(*types.PersistentPayment); ok {
		return payment.EntID
	}
	return ent.(*paymentmwpb.Payment).EntID
}
