package withdraw

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	withdrawmgrcli "github.com/NpoolPlatform/ledger-manager/pkg/client/withdraw"
	withdrawmgrpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/withdraw"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"

	ledgerdetailmgrpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"

	commonpb "github.com/NpoolPlatform/message/npool"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	"github.com/shopspring/decimal"
)

func processWithdraw(ctx context.Context, withdraw *withdrawmgrpb.Withdraw) error {
	tx, err := billingcli.GetTransaction(ctx, withdraw.PlatformTransactionID)
	if err != nil {
		return err
	}

	unlocked := decimal.NewFromInt(0)
	outcoming := decimal.NewFromInt(0)
	state := withdraw.State

	unlocked, err = decimal.NewFromString(withdraw.Amount)
	if err != nil {
		return err
	}

	// If tx done, unlock balance with outcoming, or unlock balance without outcoming
	switch tx.State {
	case billingconst.CoinTransactionStateFail:
		state = withdrawmgrpb.WithdrawState_TransactionFail
	case billingconst.CoinTransactionStateSuccessful:
		state = withdrawmgrpb.WithdrawState_Successful
	default:
		return nil
	}

	if tx.State == billingconst.CoinTransactionStateSuccessful {
		outcoming = unlocked
	}

	if err := ledgermwcli.UnlockBalance(
		ctx,
		withdraw.AppID, withdraw.UserID, withdraw.CoinTypeID,
		ledgerdetailmgrpb.IOSubType_Withdrawal,
		unlocked, outcoming,
		fmt.Sprintf(
			`{"WithdrawID":"%v","TransactionID":"%v","CID":"%v"}`,
			withdraw.ID,
			withdraw.PlatformTransactionID,
			tx.ChainTransactionID,
		),
	); err != nil {
		return err
	}

	// Update withdraw state
	u := &withdrawmgrpb.WithdrawReq{
		ID:                 &withdraw.ID,
		State:              &state,
		ChainTransactionID: &tx.ChainTransactionID,
	}
	_, err = withdrawmgrcli.UpdateWithdraw(ctx, u)
	return err
}

func processWithdraws(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	for {
		withdraws, _, err := withdrawmgrcli.GetWithdraws(ctx, &withdrawmgrpb.Conds{
			State: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(withdrawmgrpb.WithdrawState_Transferring),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Infow("processWithdraws", "error", err)
			return
		}

		// TODO: batch get transaction

		for _, withdraw := range withdraws {
			if err := processWithdraw(ctx, withdraw); err != nil {
				logger.Sugar().Infow("processWithdraws", "error", err)
				return
			}
		}

		offset += 1000
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)

	for {
		processWithdraws(ctx)
		<-ticker.C
	}
}
