package benefit

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

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

	_, ok = st.PlatformAccounts[coinTypeID]
	if !ok {
		st.PlatformAccounts[coinTypeID] = map[accountmgrpb.AccountUsedFor]*pltfaccmwpb.Account{}
	}
	st.PlatformAccounts[coinTypeID][usedFor] = acc

	return acc, nil
}

func (st *State) TransferReward(ctx context.Context, good *Good) error { //nolint
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

	coin, err := coinmwcli.GetCoin(ctx, good.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	leastTransferAmount, err := decimal.NewFromString(coin.LeastTransferAmount)
	if err != nil {
		return err
	}
	if leastTransferAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid least transfer amount")
	}

	txs := []*txmgrpb.TxReq{}

	toUser := decimal.NewFromInt(0)
	toPlatform := decimal.NewFromInt(0)

	if good.UserRewardAmount.Cmp(leastTransferAmount) > 0 {
		toUser = good.UserRewardAmount
	}
	if good.TodayRewardAmount.Sub(good.UserRewardAmount).Cmp(leastTransferAmount) > 0 {
		toPlatform = good.TodayRewardAmount.Sub(good.UserRewardAmount)
	}

	if toUser.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := toUser.String()
		feeAmount := decimal.NewFromInt(0).String()
		txExtra := fmt.Sprintf(
			`{"GoodID":"%v","Reward":"%v","UserReward":"%v","PlatformReward":"%v","TechniqueServiceFee":"%v"}`,
			good.ID,
			good.TodayRewardAmount,
			good.UserRewardAmount,
			good.PlatformRewardAmount,
			good.TechniqueServiceFeeAmount,
		)
		txType := basetypes.TxType_TxUserBenefit
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
		txType := basetypes.TxType_TxPlatformBenefit
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

	reservedAmount, err := decimal.NewFromString(coin.ReservedAmount)
	if err != nil {
		return err
	}

	nextStartAmount := good.BenefitAccountAmount.Sub(reservedAmount)
	state := goodmgrpb.BenefitState_BenefitTransferring
	nextStartAmountS := nextStartAmount.String()
	lastBenefitAmountS := good.TodayRewardAmount.String()

	req := &goodmwpb.GoodReq{
		ID:                     &good.ID,
		BenefitState:           &state,
		NextBenefitStartAmount: &nextStartAmountS,
		LastBenefitAmount:      &lastBenefitAmountS,
	}
	g, err := goodmwcli.UpdateGood(ctx, req)
	if err != nil {
		return err
	}

	if good.UserRewardAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		ords := []*ordermwpb.OrderReq{}
		for _oid, _pid := range good.BenefitOrderIDs {
			oid := _oid
			pid := _pid
			ords = append(ords, &ordermwpb.OrderReq{
				ID:            &oid,
				PaymentID:     &pid,
				LastBenefitAt: &g.LastBenefitAt,
			})
		}
		if len(ords) > 0 {
			logger.Sugar().Infow("TransferReward",
				"GoodID", good.ID,
				"UserRewardAmount", good.UserRewardAmount,
				"Units", good.BenefitOrderUnits,
				"Orders", len(good.BenefitOrderIDs),
				"UpdateOrders", len(ords),
				"LastBenefitAt", g.LastBenefitAt,
			)
			_, err := ordermwcli.UpdateOrders(ctx, ords)
			if err != nil {
				return err
			}
		}
	}

	if len(txs) == 0 {
		return nil
	}

	infos, err := txmwcli.CreateTxs(ctx, txs)
	if err != nil {
		return err
	}

	req.BenefitState = nil
	for _, tx := range infos {
		req.BenefitTIDs = append(req.BenefitTIDs, tx.ID)
	}
	_, err = goodmwcli.UpdateGood(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (st *State) CheckTransfer(ctx context.Context, good *Good) error {
	transferred := decimal.NewFromInt(0)

	if len(good.BenefitTIDs) > 0 {
		txs, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
			IDs: &commonpb.StringSliceVal{
				Op:    cruder.IN,
				Value: good.BenefitTIDs,
			},
		}, int32(0), int32(len(good.BenefitTIDs)))
		if err != nil {
			return err
		}

		for _, tx := range txs {
			switch tx.Type {
			case basetypes.TxType_TxPlatformBenefit:
			case basetypes.TxType_TxUserBenefit:
			default:
				return fmt.Errorf("invalid tx type")
			}

			switch tx.State {
			case txmgrpb.TxState_StateCreated:
				fallthrough //nolint
			case txmgrpb.TxState_StateWait:
				fallthrough //nolint
			case txmgrpb.TxState_StateTransferring:
				return nil
			case txmgrpb.TxState_StateSuccessful:
				fallthrough //nolint
			case txmgrpb.TxState_StateFail:
				amount, err := decimal.NewFromString(tx.Amount)
				if err != nil {
					return err
				}
				transferred = transferred.Add(amount)
			}
		}
	}

	nextStart, err := decimal.NewFromString(good.NextBenefitStartAmount)
	if err != nil {
		return err
	}

	nextStart = nextStart.Sub(transferred)
	if nextStart.Cmp(decimal.NewFromInt(0)) < 0 {
		return fmt.Errorf("invalid start amount")
	}

	state := goodmgrpb.BenefitState_BenefitBookKeeping
	nextStartS := nextStart.String()

	req := &goodmwpb.GoodReq{
		ID:                     &good.ID,
		BenefitState:           &state,
		NextBenefitStartAmount: &nextStartS,
	}
	_, err = goodmwcli.UpdateGood(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
