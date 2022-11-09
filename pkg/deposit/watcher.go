//nolint:dupl
package deposit

import (
	"context"
	"fmt"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	uuid1 "github.com/NpoolPlatform/go-service-framework/pkg/const/uuid"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	depositmgrcli "github.com/NpoolPlatform/account-manager/pkg/client/deposit"
	depositmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	depositmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/deposit"
	depositmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"

	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	commonpb "github.com/NpoolPlatform/message/npool"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func depositOne(ctx context.Context, acc *depositmwpb.Account) error {
	if acc.Locked {
		return nil
	}

	incoming, _ := decimal.NewFromString(acc.Incoming)   //nolint
	outcoming, _ := decimal.NewFromString(acc.Outcoming) //nolint

	coin, err := coininfocli.GetCoinInfo(ctx, acc.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	if err := accountlock.Lock(acc.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(acc.AccountID) //nolint
	}()

	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: acc.Address,
	})
	if err != nil {
		return err
	}
	if bal == nil {
		return fmt.Errorf("fail get balance")
	}

	balance, err := decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}

	if balance.Cmp(incoming.Sub(outcoming)) <= 0 {
		return nil
	}

	amount := balance.Sub(incoming.Sub(outcoming)).String()

	// TODO: move add and book keeping to TX

	_, err = depositmgrcli.AddAccount(ctx, &depositmgrpb.AccountReq{
		ID:        &acc.ID,
		AppID:     &acc.AppID,
		UserID:    &acc.UserID,
		AccountID: &acc.AccountID,
		Incoming:  &amount,
	})
	if err != nil {
		return err
	}

	ioExtra := fmt.Sprintf(
		`{"AppID":"%v","UserID":"%v","AccountID":"%v","CoinName":"%v","Address":"%v","Date":"%v"}`,
		acc.AppID,
		acc.UserID,
		acc.AccountID,
		coin.Name,
		acc.Address,
		time.Now(),
	)
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Deposit

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &acc.AppID,
		UserID:     &acc.UserID,
		CoinTypeID: &acc.CoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
}

func deposit(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	logger.Sugar().Infow("deposit", "Start", "...")

	for {
		accs, err := depositmwcli.GetAccounts(ctx, &depositmwpb.Conds{
			Locked: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: false,
			},
			ScannableAt: &commonpb.Uint32Val{
				Op:    cruder.LT,
				Value: uint32(time.Now().Unix()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("deposit", "error", err)
			return
		}
		if len(accs) == 0 {
			logger.Sugar().Infow("deposit", "Done", "...")
			return
		}

		for _, acc := range accs {
			if err := depositOne(ctx, acc); err != nil {
				logger.Sugar().Errorw("deposit", "error", err)
			}
		}

		offset += limit
	}
}

func tryTransferOne(ctx context.Context, acc *depositmwpb.Account) error {
	if acc.Locked {
		return nil
	}

	tx, err := billingcli.GetTransaction(ctx, acc.CollectingTID)
	if err != nil {
		return err
	}
	if tx != nil {
		switch tx.State {
		case billingconst.CoinTransactionStateSuccessful:
		case billingconst.CoinTransactionStateFail:
		default:
			return nil
		}
	}

	incoming, _ := decimal.NewFromString(acc.Incoming)   //nolint
	outcoming, _ := decimal.NewFromString(acc.Outcoming) //nolint

	coin, err := coininfocli.GetCoinInfo(ctx, acc.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	setting, err := billingcli.GetCoinSetting(ctx, acc.CoinTypeID)
	if err != nil || setting == nil {
		return nil
	}

	limit := setting.PaymentAccountCoinAmount
	if incoming.Sub(outcoming).Cmp(decimal.NewFromFloat(limit)) < 0 {
		return nil
	}

	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: acc.Address,
	})
	if err != nil {
		return err
	}
	if bal == nil {
		return fmt.Errorf("fail get balance")
	}

	balance, err := decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}

	if balance.Cmp(incoming.Sub(outcoming)) < 0 {
		return fmt.Errorf("insufficient funds")
	}
	if incoming.Sub(outcoming).Cmp(decimal.NewFromFloat(coin.ReservedAmount)) <= 0 {
		return nil
	}

	if err := accountlock.Lock(acc.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(acc.AccountID) //nolint
	}()

	amount := incoming.Sub(outcoming).Sub(decimal.NewFromFloat(coin.ReservedAmount))

	tx, err = billingcli.CreateTransaction(ctx, &billingpb.CoinAccountTransaction{
		AppID:              acc.AppID,
		UserID:             acc.UserID,
		GoodID:             uuid.UUID{}.String(),
		FromAddressID:      acc.AccountID,
		ToAddressID:        setting.GoodIncomingAccountID,
		CoinTypeID:         acc.CoinTypeID,
		Amount:             amount.InexactFloat64(),
		Message:            fmt.Sprintf("deposit collecting of %v at %v", acc.Address, time.Now()),
		ChainTransactionID: uuid.New().String(),
		CreatedFor:         billingconst.TransactionForCollecting,
	})
	if err != nil {
		return err
	}

	locked := true
	lockedBy := accountmgrpb.LockedBy_Collecting

	_, err = depositmwcli.UpdateAccount(ctx, &depositmwpb.AccountReq{
		ID:            &acc.ID,
		AppID:         &acc.AppID,
		UserID:        &acc.UserID,
		CoinTypeID:    &acc.CoinTypeID,
		AccountID:     &acc.AccountID,
		Locked:        &locked,
		LockedBy:      &lockedBy,
		CollectingTID: &tx.ID,
	})
	return err
}

func transfer(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	logger.Sugar().Infow("transfer", "Start", "...")

	for {
		// TODO: only get active / unlocked / unblocked accounts
		accs, err := depositmwcli.GetAccounts(ctx, &depositmwpb.Conds{
			Locked: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: false,
			},
			ScannableAt: &commonpb.Uint32Val{
				Op:    cruder.LT,
				Value: uint32(time.Now().Unix()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("deposit", "error", err)
			return
		}
		if len(accs) == 0 {
			logger.Sugar().Infow("transfer", "Done", "...")
			return
		}

		for _, acc := range accs {
			if err := tryTransferOne(ctx, acc); err != nil {
				logger.Sugar().Errorw("transfer", "error", err)
			}
		}

		offset += limit
	}
}

func tryFinishOne(ctx context.Context, acc *depositmwpb.Account) error {
	tx, err := billingcli.GetTransaction(ctx, acc.CollectingTID)
	if err != nil {
		return err
	}
	if tx == nil {
		return nil
	}

	outcoming := decimal.NewFromInt(0)

	switch tx.State {
	case billingconst.CoinTransactionStateSuccessful:
		outcoming = outcoming.Add(decimal.NewFromFloat(tx.Amount))
	case billingconst.CoinTransactionStateFail:
	default:
		return nil
	}

	if err := accountlock.Lock(acc.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(acc.AccountID) //nolint
	}()

	scannableAt := uint32(time.Now().Unix() + timedef.SecondsPerHour)

	locked := false
	lockedBy := accountmgrpb.LockedBy_DefaultLockedBy
	collectingID := uuid1.InvalidUUIDStr
	outcomingS := outcoming.String()

	req := &depositmwpb.AccountReq{
		ID:            &acc.ID,
		AppID:         &acc.AppID,
		UserID:        &acc.UserID,
		CoinTypeID:    &acc.CoinTypeID,
		AccountID:     &acc.AccountID,
		Locked:        &locked,
		LockedBy:      &lockedBy,
		CollectingTID: &collectingID,
		ScannableAt:   &scannableAt,
	}
	if outcoming.Cmp(decimal.NewFromInt(0)) > 0 {
		req.Outcoming = &outcomingS
	}

	_, err = depositmwcli.UpdateAccount(ctx, req)
	return err
}

func finish(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	logger.Sugar().Infow("finish", "Start", "...")

	for {
		// TODO: only get active / unlocked / unblocked accounts
		accs, err := depositmwcli.GetAccounts(ctx, &depositmwpb.Conds{
			Locked: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: true,
			},
			LockedBy: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(accountmgrpb.LockedBy_Collecting),
			},
			ScannableAt: &commonpb.Uint32Val{
				Op:    cruder.LT,
				Value: uint32(time.Now().Unix()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("deposit", "error", err)
			return
		}
		if len(accs) == 0 {
			logger.Sugar().Infow("finish", "Done", "...")
			return
		}
		for _, acc := range accs {
			if err := tryFinishOne(ctx, acc); err != nil {
				logger.Sugar().Errorw("finish", "error", err)
			}
		}

		offset += limit
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)

	for {
		select {
		case <-ticker.C:
			deposit(ctx)
			transfer(ctx)
			finish(ctx)
		case <-ctx.Done():
			return
		}
	}
}
