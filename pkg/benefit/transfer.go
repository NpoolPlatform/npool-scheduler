package benefit

import (
	"context"
	"fmt"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	// coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
)

func (st *State) platformAccount(
	ctx context.Context,
	coinTypeID string,
	usedFor accountmgrpb.AccountUsedFor,
) (
	*pltfaccmwpb.Account,
	error,
) {
	accs, ok := st.PlatformAccounts[coinTypeID]
	if ok {
		acc, ok := accs[usedFor]
		if ok {
			return acc, nil
		}
	}

	acc, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: coinTypeID,
		},
		UsedFor: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(usedFor),
		},
		Backup: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
	})
	if err != nil {
		return nil, err
	}
	if acc == nil {
		return nil, fmt.Errorf("invalid account")
	}

	accs, ok = st.PlatformAccounts[coinTypeID]
	if !ok {
		st.PlatformAccounts[coinTypeID] = map[accountmgrpb.AccountUsedFor]*pltfaccmwpb.Account{}
	}
	st.PlatformAccounts[coinTypeID][usedFor] = acc

	return acc, nil
}

func (st *State) TransferReward(ctx context.Context, good *Good) error {
	return nil
}
