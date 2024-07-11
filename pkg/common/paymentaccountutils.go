//nolint:dupl
package common

import (
	"context"

	paymentaccountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	"github.com/google/uuid"
)

func GetPaymentAccounts(ctx context.Context, accountIDs []string) (map[string]*paymentaccountmwpb.Account, error) {
	for _, accountID := range accountIDs {
		if _, err := uuid.Parse(accountID); err != nil {
			return nil, wlog.WrapError(err)
		}
	}

	paymentAccounts, _, err := paymentaccountmwcli.GetAccounts(ctx, &paymentaccountmwpb.Conds{
		AccountIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: accountIDs},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		LockedBy:   &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountLockedBy_Payment)},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	}, int32(0), int32(len(accountIDs)))
	if err != nil {
		return nil, wlog.WrapError(err)
	}
	paymentAccountMap := map[string]*paymentaccountmwpb.Account{}
	for _, paymentAccount := range paymentAccounts {
		paymentAccountMap[paymentAccount.AccountID] = paymentAccount
	}
	return paymentAccountMap, nil
}
