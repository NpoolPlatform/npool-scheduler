//nolint:dupl
package common

import (
	"context"

	platformaccountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	platformaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	"github.com/google/uuid"
)

func GetCoinPlatformAccounts(ctx context.Context, usedFor basetypes.AccountUsedFor, coinTypeIDs []string) (map[string]*platformaccountmwpb.Account, error) {
	for _, coinTypeID := range coinTypeIDs {
		if _, err := uuid.Parse(coinTypeID); err != nil {
			return nil, wlog.WrapError(err)
		}
	}

	platformAccounts, _, err := platformaccountmwcli.GetAccounts(ctx, &platformaccountmwpb.Conds{
		CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
		UsedFor:     &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(usedFor)},
		Backup:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:      &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Locked:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	}, int32(0), int32(len(coinTypeIDs)))
	if err != nil {
		return nil, wlog.WrapError(err)
	}
	platformAccountMap := map[string]*platformaccountmwpb.Account{}
	for _, platformAccount := range platformAccounts {
		platformAccountMap[platformAccount.CoinTypeID] = platformAccount
	}
	return platformAccountMap, nil
}
