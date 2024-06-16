//nolint:dupl
package common

import (
	"context"

	goodbenefitaccountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/goodbenefit"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodbenefitaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	"github.com/google/uuid"
)

func GetGoodCoinBenefitAccounts(ctx context.Context, goodID string, coinTypeIDs []string) (map[string]*goodbenefitaccountmwpb.Account, error) {
	for _, coinTypeID := range coinTypeIDs {
		if _, err := uuid.Parse(coinTypeID); err != nil {
			return nil, wlog.WrapError(err)
		}
	}

	goodBenefitAccounts, _, err := goodbenefitaccountmwcli.GetAccounts(ctx, &goodbenefitaccountmwpb.Conds{
		GoodID:      &basetypes.StringVal{Op: cruder.EQ, Value: goodID},
		CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
		Backup:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:      &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Blocked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	}, int32(0), int32(len(coinTypeIDs)))
	if err != nil {
		return nil, wlog.WrapError(err)
	}
	goodBenefitAccountMap := map[string]*goodbenefitaccountmwpb.Account{}
	for _, goodBenefitAccount := range goodBenefitAccounts {
		goodBenefitAccountMap[goodBenefitAccount.CoinTypeID] = goodBenefitAccount
	}
	return goodBenefitAccountMap, nil
}
