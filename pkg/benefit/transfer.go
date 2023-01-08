package benefit

import (
	"context"
	"fmt"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	// coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
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
	if good.TodayRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	userHotAcc, err := st.platformAccount(
		ctx,
		good.CoinTypeID,
		accountmgrpb.AccountUsedFor_UserBenefitHot)
	if err != nil {
		return err
	}

	pltfColdAcc, err := st.platformAccount(
		ctx,
		good.CoinTypeID,
		accountmgrpb.AccountUsedFor_PlatformBenefitCold)
	if err != nil {
		return err
	}

	goodBenefitAcc, err := st.goodBenefit(ctx, good)
	if err != nil {
		return err
	}

	txs := []*txmgrpb.TxReq{}

	if good.UserRewardAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := good.UserRewardAmount.String()
		feeAmount := decimal.NewFromInt(0).String()
		txExtra := fmt.Sprintf(
			`{"GoodID":"%v","Reward":"%v","UserReward":"%v","PlatformReward":"%v","TechniqueServiceFee":"%v"}`,
			good.ID,
			good.TodayRewardAmount,
			good.UserRewardAmount,
			good.PlatformRewardAmount,
			good.TechniqueServiceFeeAmount,
		)
		txType := txmgrpb.TxType_TxUserBenefit
		txs = append(txs, &txmgrpb.TxReq{
			CoinTypeID:    &good.CoinTypeID,
			FromAccountID: &goodBenefitAcc.AccountID,
			ToAccountID:   &userHotAcc.AccountID,
			Amount:        &amount,
			FeeAmount:     &feeAmount,
			Extra:         &txExtra,
			Type:          &txType,
		})
	}

	toPlatform := good.PlatformRewardAmount.Add(good.TechniqueServiceFeeAmount)

	if toPlatform.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := toPlatform.String()
		feeAmount := decimal.NewFromInt(0).String()
		txExtra := fmt.Sprintf(
			`{"GoodID":"%v","Reward":"%v","UserReward":"%v","PlatformReward":"%v","TechniqueServiceFee":"%v"}`,
			good.ID,
			good.TodayRewardAmount,
			good.UserRewardAmount,
			good.PlatformRewardAmount,
			good.TechniqueServiceFeeAmount,
		)
		txType := txmgrpb.TxType_TxPlatformBenefit
		txs = append(txs, &txmgrpb.TxReq{
			CoinTypeID:    &good.CoinTypeID,
			FromAccountID: &goodBenefitAcc.AccountID,
			ToAccountID:   &pltfColdAcc.AccountID,
			Amount:        &amount,
			FeeAmount:     &feeAmount,
			Extra:         &txExtra,
			Type:          &txType,
		})
	}
	if len(txs) == 0 {
		return nil
	}

	state := goodmgrpb.BenefitState_BenefitTransferring
	req := &goodmwpb.GoodReq{
		ID:           &good.ID,
		BenefitState: &state,
	}
	_, err = goodmwcli.UpdateGood(ctx, req)
	if err != nil {
		return err
	}

	infos, err := txmwcli.CreateTxs(ctx, txs)
	if err != nil {
		return err
	}

	for _, tx := range infos {
		req.BenefitTIDs = append(req.BenefitTIDs, tx.ID)
	}
	_, err = goodmwcli.UpdateGood(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
