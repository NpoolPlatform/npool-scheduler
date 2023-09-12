package sentinel

import (
	"context"
	"time"

	depositaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user/types"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

// Potential issue: if this account cannot be processed within one interval, then we may add
//   deposit amount for this account again
func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		accounts, _, err := depositaccmwcli.GetAccounts(ctx, &depositaccmwpb.Conds{
			Locked:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
			ScannableAt: &basetypes.Uint32Val{Op: cruder.LT, Value: uint32(time.Now().Unix())},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(accounts) == 0 {
			return nil
		}

		for _, account := range accounts {
			cancelablefeed.CancelableFeed(ctx, account, exec)
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
	if account, ok := ent.(*types.PersistentAccount); ok {
		return account.ID
	}
	return ent.(*depositaccmwpb.Account).ID
}
