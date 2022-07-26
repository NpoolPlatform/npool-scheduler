package transaction

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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func onCreatedChecker(ctx context.Context) {
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

	waits = append(waits, payings...)
	thisWait := map[string]struct{}{}

tryWaitOne:
	for _, created := range createds {
		if _, ok := thisWait[created.FromAddressID]; ok {
			continue
		}

		logger.Sugar().Infow("transaction", "id", created.ID, "amount", created.Amount, "state", created.State)

		for _, waited := range waits {
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

func transfer(ctx context.Context, tx *billingpb.CoinAccountTransaction) error {
	logger.Sugar().Infow("transaction", "id", tx.ID, "amount", tx.Amount, "state", tx.State)

	from, err := billingcli.GetAccount(ctx, tx.FromAddressID)
	if err != nil {
		return fmt.Errorf("fail get account: %v", err)
	}
	if from == nil {
		return fmt.Errorf("invalid from address")
	}

	to, err := billingcli.GetAccount(ctx, tx.ToAddressID)
	if err != nil {
		return fmt.Errorf("fail get account: %v", err)
	}
	if to == nil {
		return fmt.Errorf("invalid from address")
	}

	coin, err := coininfocli.GetCoinInfo(ctx, tx.CoinTypeID)
	if err != nil {
		return fmt.Errorf("fail get coininfo: %v", err)
	}
	if coin == nil {
		return fmt.Errorf("invalid coininfo")
	}

	logger.Sugar().Infow("transaction", "id", tx.ID,
		"coin", coin.Name, "from", from.Address, "to", to.Address,
		"amount", tx.Amount, "fee", tx.TransactionFee)

	err = sphinxproxycli.CreateTransaction(ctx, &sphinxproxypb.CreateTransactionRequest{
		TransactionID: tx.ID,
		Name:          coin.Name,
		Amount:        tx.Amount - tx.TransactionFee,
		From:          from.Address,
		To:            to.Address,
	})
	if err != nil {
		return fmt.Errorf("fail transfer: %v", err)
	}

	return nil
}

func onWaitChecker(ctx context.Context) {
	waits, err := billingcli.GetTransactions(ctx, billingconst.CoinTransactionStateWait)
	if err != nil {
		logger.Sugar().Errorw("transaction", "state", billingconst.CoinTransactionStateWait, "error", err)
		return
	}

	for _, wait := range waits {
		if err := transfer(ctx, wait); err != nil {
			logger.Sugar().Errorw("transaction", "id", wait.ID, "error", err)
			return
		}

		wait.State = billingconst.CoinTransactionStatePaying
		_, err := billingcli.UpdateTransaction(ctx, wait)
		if err != nil {
			logger.Sugar().Errorw("transaction", "id", wait.ID, "error", err)
			return
		}
	}
}

func onPayingChecker(ctx context.Context) {
	payings, err := billingcli.GetTransactions(ctx, billingconst.CoinTransactionStatePaying)
	if err != nil {
		logger.Sugar().Errorw("transaction", "state", billingconst.CoinTransactionStatePaying, "error", err)
		return
	}

	for _, paying := range payings {
		toState := billingconst.CoinTransactionStatePaying
		cid := ""

		tx, err := sphinxproxycli.GetTransaction(ctx, paying.ID)
		if err != nil {
			logger.Sugar().Errorw("transaction", "id", paying.ID, "error", err)
			switch status.Code(err) {
			case codes.InvalidArgument:
				toState = billingconst.CoinTransactionStateFail
			case codes.NotFound:
				toState = billingconst.CoinTransactionStateFail
			default:
				continue
			}
		}

		if tx == nil {
			logger.Sugar().Errorw("transaction", "id", paying.ID, "error", "invalid transaction id")
		}

		if toState == billingconst.CoinTransactionStatePaying {
			switch tx.TransactionState {
			case sphinxproxypb.TransactionState_TransactionStateFail:
				toState = billingconst.CoinTransactionStateFail
				paying.FailHold = true
			case sphinxproxypb.TransactionState_TransactionStateDone:
				toState = billingconst.CoinTransactionStateSuccessful
				if tx.CID == "" {
					paying.Message = fmt.Sprintf("%v (successful without CID)", paying.Message)
					toState = billingconst.CoinTransactionStateFail
					paying.FailHold = true
				}
				cid = tx.CID
			default:
				continue
			}
		}

		paying.State = toState
		paying.ChainTransactionID = cid

		logger.Sugar().Infow("transaction", "id", paying.ID, "amount", paying.Amount,
			"state", paying.State, "toState", toState, "cid", cid)
		_, err = billingcli.UpdateTransaction(ctx, paying)
		if err != nil {
			logger.Sugar().Errorw("transaction", "id", paying.ID, "error", err)
		}
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		onCreatedChecker(ctx)
		onWaitChecker(ctx)
		onPayingChecker(ctx)
	}
}
