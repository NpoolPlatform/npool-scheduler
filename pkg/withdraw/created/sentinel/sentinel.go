package sentinel

import (
	"context"
	"fmt"

	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
	withdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		withdraws, _, err := withdrawmwcli.GetWithdraws(ctx, &withdrawmwpb.Conds{
			State: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ledgertypes.WithdrawState_Created)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(withdraws) == 0 {
			return nil
		}

		for _, withdraw := range withdraws {
			key := fmt.Sprintf(
				"%v:%v:%v:%v",
				basetypes.Prefix_PrefixCreateWithdraw,
				withdraw.AppID,
				withdraw.UserID,
				withdraw.ID,
			)
			if err := redis2.TryLock(key, 0); err != nil {
				continue
			}
			_ = redis2.Unlock(key) // nolint
			exec <- withdraw
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
	return ent.(*withdrawmwpb.Withdraw).ID
}
