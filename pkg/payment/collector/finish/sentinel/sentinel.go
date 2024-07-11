package sentinel

import (
	"context"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/collector/finish/types"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		accounts, _, err := payaccmwcli.GetAccounts(ctx, &payaccmwpb.Conds{
			Locked:   &basetypes.BoolVal{Op: cruder.EQ, Value: true},
			LockedBy: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountLockedBy_Collecting)},
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
		return account.EntID
	}
	return ent.(*payaccmwpb.Account).EntID
}
