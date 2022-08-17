package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func checkGoodPayment(ctx context.Context, payment *billingpb.GoodPayment) { //nolint
	if !payment.Idle {
		return
	}

	if payment.AvailableAt >= uint32(time.Now().Unix()) {
		return
	}

	coin, err := coininfocli.GetCoinInfo(ctx, payment.PaymentCoinTypeID)
	if err != nil || coin == nil {
		logger.Sugar().Errorw("checkGoodPayment", "payment", payment.ID, "error", err)
		return
	}

	if err := accountlock.Lock(payment.AccountID); err != nil {
		logger.Sugar().Errorw("checkGoodPayment", "payment", payment.ID, "error", err)
		return
	}
	defer func() {
		_ = accountlock.Unlock(payment.AccountID) //nolint
	}()

	payment, err = billingcli.GetGoodPayment(ctx, payment.ID)
	if err != nil {
		logger.Sugar().Errorw("checkGoodPayment", "payment", payment.ID, "error", err)
		return
	}

	if !payment.Idle {
		return
	}

	if payment.AvailableAt >= uint32(time.Now().Unix()) {
		return
	}

	account, err := billingcli.GetAccount(ctx, payment.AccountID)
	if err != nil || account == nil {
		logger.Sugar().Errorw("checkGoodPayment", "AccountID", payment.AccountID, "account", account, "error", err)
		return
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: account.Address,
	})
	if err != nil || balance == nil {
		logger.Sugar().Errorw(
			"checkGoodPayment",
			"balance", balance,
			"payment", payment.ID,
			"coin", coin.Name,
			"address", account.Address,
			"error", err,
		)
		return
	}

	setting, err := billingcli.GetCoinSetting(ctx, payment.PaymentCoinTypeID)
	if err != nil || setting == nil {
		logger.Sugar().Errorw("checkGoodPayment", "payment", payment.ID, "error", err)
		return
	}

	limit := setting.PaymentAccountCoinAmount
	logger.Sugar().Infow("checkGoodPayment", "limit", limit, "coin", coin.Name,
		"balance", balance.BalanceStr, "reserved", coin.ReservedAmount,
		"account", account.Address, "accountID", payment.AccountID)
	bal, err := decimal.NewFromString(balance.BalanceStr)
	if err != nil {
		logger.Sugar().Errorw("checkGoodPayment", "error", err)
		return
	}

	if bal.Cmp(decimal.NewFromFloat(limit)) <= 0 {
		return
	}
	if bal.Cmp(decimal.NewFromFloat(coin.ReservedAmount)) <= 0 {
		return
	}

	tx, err := billingcli.CreateTransaction(ctx, &billingpb.CoinAccountTransaction{
		AppID:              uuid.UUID{}.String(),
		UserID:             uuid.UUID{}.String(),
		GoodID:             uuid.UUID{}.String(),
		FromAddressID:      payment.AccountID,
		ToAddressID:        setting.GoodIncomingAccountID,
		CoinTypeID:         coin.ID,
		Amount:             bal.Sub(decimal.NewFromFloat(coin.ReservedAmount)).InexactFloat64(),
		Message:            fmt.Sprintf("payment collecting transfer of %v at %v", payment.GoodID, time.Now()),
		ChainTransactionID: uuid.New().String(),
		CreatedFor:         billingconst.TransactionForCollecting,
	})
	if err != nil {
		logger.Sugar().Errorw("checkGoodPayment", "payment", payment.ID, "error", err)
		return
	}

	payment.Idle = false
	payment.OccupiedBy = billingconst.TransactionForCollecting
	payment.UsedFor = billingconst.TransactionForCollecting
	payment.CollectingTID = tx.ID
	_, err = billingcli.UpdateGoodPayment(ctx, payment)
	if err != nil {
		logger.Sugar().Errorw("checkGoodPayment", "error", err)
	}
}

func checkGoodPayments(ctx context.Context) {
	payments, err := billingcli.GetGoodPayments(ctx, cruder.NewFilterConds())
	if err != nil {
		logger.Sugar().Errorw("checkGoodPayments", "error", err)
		return
	}

	for _, payment := range payments {
		checkGoodPayment(ctx, payment)
	}
}

// nolint
func checkCollectingPayments(ctx context.Context) {
	payments, err := billingcli.GetGoodPayments(ctx, cruder.NewFilterConds())
	if err != nil {
		logger.Sugar().Errorw("checkCollectingPayments", "error", err)
		return
	}

	for _, payment := range payments {
		if payment.Idle {
			continue
		}

		err = accountlock.Lock(payment.AccountID)
		if err != nil {
			logger.Sugar().Errorw("checkCollectingPayments", "AccountID", payment.AccountID, "error", err)
			continue
		}

		unlock := func() {
			if err := accountlock.Unlock(payment.AccountID); err != nil {
				logger.Sugar().Errorw("checkCollectingPayments", "error", err)
			}
		}

		payment, err = billingcli.GetGoodPayment(ctx, payment.ID)
		if err != nil {
			unlock()
			return
		}

		if payment.Idle {
			unlock()
			continue
		}

		if payment.UsedFor != billingconst.TransactionForCollecting ||
			payment.OccupiedBy != billingconst.TransactionForCollecting {
			unlock()
			continue
		}

		tx, err := billingcli.GetTransaction(ctx, payment.CollectingTID)
		if err != nil {
			logger.Sugar().Errorw("checkCollectingPayments", "error", err)
			unlock()
			continue
		}
		if tx == nil {
			logger.Sugar().Errorw("checkCollectingPayments", "CollectingTID", payment.CollectingTID)
			unlock()
			continue
		}

		switch tx.State {
		case billingconst.CoinTransactionStateCreated:
			fallthrough //nolint
		case billingconst.CoinTransactionStateWait:
			fallthrough //nolint
		case billingconst.CoinTransactionStatePaying:
			unlock()
			continue
		}

		payment.Idle = true
		payment.OccupiedBy = billingconst.TransactionForNotUsed
		payment.UsedFor = billingconst.TransactionForNotUsed
		_, err = billingcli.UpdateGoodPayment(ctx, payment)
		if err != nil {
			logger.Sugar().Errorw("checkCollectingPayments", "error", err)
		}
		unlock()
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		checkGoodPayments(ctx)
		checkCollectingPayments(ctx)
	}
}
