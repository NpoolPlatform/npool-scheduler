package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
)

func checkGoodPayment(ctx context.Context, account *payaccmwpb.Account) error { //nolint
	if account.AvailableAt >= uint32(time.Now().Unix()) {
		return nil
	}

	coin, err := coinmwcli.GetCoin(ctx, account.CoinTypeID)
	if err != nil {
		return err
	}

	if err := accountlock.Lock(account.AccountID); err != nil {
		logger.Sugar().Errorw("checkGoodPayment", "account", account.AccountID, "error", err)
		return nil
	}
	defer func() {
		_ = accountlock.Unlock(account.AccountID) //nolint
	}()

	_acc, err := pltfaccmwcli.GetAccount(ctx, account.ID)
	if err != nil {
		return err
	}
	if _acc.Locked || _acc.Blocked || !_acc.Active {
		return nil
	}
	if account.AvailableAt >= uint32(time.Now().Unix()) {
		return nil
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: account.Address,
	})
	if err != nil {
		return err
	}
	if balance == nil {
		return fmt.Errorf("invalid balance")
	}

	limit, err := decimal.NewFromString(coin.PaymentAccountCollectAmount)
	if err != nil {
		return err
	}

	reserved, err := decimal.NewFromString(coin.ReservedAmount)
	if err != nil {
		return err
	}

	feeAmount, err := decimal.NewFromString(coin.CollectFeeAmount)
	if err != nil {
		return err
	}

	logger.Sugar().Infow("checkGoodPayment", "limit", limit, "coin", coin.Name,
		"balance", balance.BalanceStr, "reserved", coin.ReservedAmount,
		"account", account.Address, "accountID", account.ID)

	bal, err := decimal.NewFromString(balance.BalanceStr)
	if err != nil {
		logger.Sugar().Errorw("checkGoodPayment", "error", err)
		return err
	}

	if bal.Cmp(limit) <= 0 {
		return fmt.Errorf("insufficient funds")
	}
	if bal.Cmp(reserved) <= 0 {
		return fmt.Errorf("insufficient funds")
	}
	if bal.Cmp(feeAmount) < 0 {
		return fmt.Errorf("insufficient gas")
	}

	if coin.ID != coin.FeeCoinTypeID {
		balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    coin.FeeCoinName,
			Address: account.Address,
		})
		if err != nil {
			return err
		}
		if balance == nil {
			return fmt.Errorf("invalid balance")
		}

		bal, err := decimal.NewFromString(balance.BalanceStr)
		if err != nil {
			return err
		}

		if bal.Cmp(feeAmount) < 0 {
			return fmt.Errorf("insufficient gas")
		}
	}

	collect, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: coin.ID,
		},
		UsedFor: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(accountmgrpb.AccountUsedFor_PaymentCollector),
		},
		Backup: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
	})
	if err != nil {
		return nil
	}
	if collect == nil {
		return fmt.Errorf("invalid collect account")
	}

	amountS := bal.Sub(reserved).String()
	feeAmountS := "0"
	txType := txmgrpb.TxType_TxPaymentCollect

	tx, err := txmwcli.CreateTx(ctx, &txmgrpb.TxReq{
		CoinTypeID:    &coin.ID,
		FromAccountID: &account.AccountID,
		ToAccountID:   &collect.AccountID,
		Amount:        &amountS,
		FeeAmount:     &feeAmountS,
		Type:          &txType,
	})
	if err != nil {
		return err
	}

	locked := true
	lockedBy := accountmgrpb.LockedBy_Collecting

	_, err = payaccmwcli.UpdateAccount(ctx, &payaccmwpb.AccountReq{
		ID:            &account.ID,
		CoinTypeID:    &account.CoinTypeID,
		Locked:        &locked,
		LockedBy:      &lockedBy,
		CollectingTID: &tx.ID,
	})
	return err
}

func checkGoodPayments(ctx context.Context) {
	offset := int32(0)
	const limit = int32(1000)

	for {
		accs, _, err := payaccmwcli.GetAccounts(ctx, &payaccmwpb.Conds{
			Active: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: true,
			},
			Locked: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: false,
			},
			Blocked: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: false,
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("checkGoodPayments", "error", err)
			return
		}
		if len(accs) == 0 {
			return
		}

		for _, acc := range accs {
			if err := checkGoodPayment(ctx, acc); err != nil {
				logger.Sugar().Errorw("checkGoodPayment", "error", err)
			}
		}

		offset += limit
	}
}

func checkCollectingPayment(ctx context.Context, account *payaccmwpb.Account) error {
	tx, err := txmwcli.GetTx(ctx, account.CollectingTID)
	if err != nil {
		return err
	}

	switch tx.State {
	case txmgrpb.TxState_StateSuccessful:
	case txmgrpb.TxState_StateFail:
	default:
		return nil
	}

	locked := false

	_, err = payaccmwcli.UpdateAccount(ctx, &payaccmwpb.AccountReq{
		ID:         &account.ID,
		CoinTypeID: &account.CoinTypeID,
		Locked:     &locked,
	})
	return err
}

// nolint
func checkCollectingPayments(ctx context.Context) {
	offset := int32(0)
	const limit = int32(1000)

	for {
		accs, _, err := payaccmwcli.GetAccounts(ctx, &payaccmwpb.Conds{
			Locked: &commonpb.BoolVal{
				Op:    cruder.EQ,
				Value: true,
			},
			LockedBy: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(accountmgrpb.LockedBy_Collecting),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("checkCollectingPayments", "error", err)
			return
		}
		if len(accs) == 0 {
			return
		}

		for _, acc := range accs {
			if err := checkCollectingPayment(ctx, acc); err != nil {
				logger.Sugar().Errorw("checkCollectingPayments", "error", err)
			}
		}

		offset += limit
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		checkGoodPayments(ctx)
		checkCollectingPayments(ctx)
	}
}
