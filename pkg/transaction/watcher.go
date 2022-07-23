package transaction

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
)

func onCreatedChecker(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	createds, err := billingcli.GetTransactions(ctx, billingconst.CoinTransactionStateCreated)
	if err != nil {
		logger.Sugar().Error("transaction", "state", billingconst.CoinTransactionStateCreated, "error", err)
		return
	}
	if len(createds) == 0 {
		return
	}

	waits, err := billingcli.GetTransactions(ctx, billingconst.CoinTransactionStateWait)
	if err != nil {
		logger.Sugar().Error("transaction", "state", billingconst.CoinTransactionStateWait, "error", err)
		return
	}

	payings, err := billingcli.GetTransactions(ctx, billingconst.CoinTransactionStatePaying)
	if err != nil {
		logger.Sugar().Error("transaction", "state", billingconst.CoinTransactionStatePaying, "error", err)
		return
	}

	waiteds := append(waits, payings...)
	thisWait := map[string]struct{}{}

tryWaitOne:
	for _, created := range createds {
		if _, ok := thisWait[created.FromAddressID]; ok {
			continue
		}

		logger.Sugar().Infow("transaction", "id", created.ID, "amount", created.Amount, "state", created.State)

		for _, waited := range waiteds {
			if created.FromAddressID == waited.FromAddressID {
				continue tryWaitOne
			}
		}

		logger.Sugar().Infow("transaction", "id", created.ID, "amount", created.Amount,
			"from", created.State, "to", billingconst.CoinTransactionStateWait)

		created.State = billingconst.CoinTransactionStateWait
		_, err := billingcli.UpdateTransaction(ctx, created)
		if err != nil {
			logger.Sugar().Error("transaction", "id", created.ID, "amount", created.Amount,
				"from", created.State, "to", billingconst.CoinTransactionStateWait, "error", err)
			return
		}

		thisWait[created.FromAddressID] = struct{}{}
	}
}

func onWaitChecker(ctx context.Context) {

}

func onPayingChecker(ctx context.Context) {

}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		onCreatedChecker(ctx)
		onWaitChecker(ctx)
		onPayingChecker(ctx)
	}
}
