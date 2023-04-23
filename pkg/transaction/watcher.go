package transaction

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"

	commonpb "github.com/NpoolPlatform/message/npool"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	accountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/account"
	useraccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/user"
	useraccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/user"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	"github.com/shopspring/decimal"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func onCreatedChecker(ctx context.Context) { //nolint
	offset := int32(0)
	const limit = int32(1000)

	ignores := map[string]struct{}{}

	for {
		createds, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
			State: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(txmgrpb.TxState_StateCreated),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("onCreatedChecker", "error", err)
			return
		}
		if len(createds) == 0 {
			return
		}

		for _, created := range createds {
			if _, ok := ignores[created.FromAccountID]; ok {
				continue
			}

			waits, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
				CoinTypeID: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: created.CoinTypeID,
				},
				AccountID: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: created.FromAccountID,
				},
				State: &commonpb.Int32Val{
					Op:    cruder.EQ,
					Value: int32(txmgrpb.TxState_StateWait),
				},
			}, int32(0), int32(1)) //nolint
			if err != nil {
				logger.Sugar().Errorw("onCreatedChecker", "error", err)
				return
			}
			if len(waits) > 0 {
				continue
			}

			payings, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
				CoinTypeID: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: created.CoinTypeID,
				},
				AccountID: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: created.FromAccountID,
				},
				State: &commonpb.Int32Val{
					Op:    cruder.EQ,
					Value: int32(txmgrpb.TxState_StateTransferring),
				},
			}, int32(0), int32(1)) //nolint
			if err != nil {
				logger.Sugar().Errorw("onCreatedChecker", "error", err)
				return
			}
			if len(payings) > 0 {
				continue
			}

			state := txmgrpb.TxState_StateWait
			_, err = txmwcli.UpdateTx(ctx, &txmgrpb.TxReq{
				ID:    &created.ID,
				State: &state,
			})
			if err != nil {
				logger.Sugar().Errorw("onCreatedChecker", "error", err)
				return
			}

			logger.Sugar().Infow("transaction", "id", created.ID, "amount", created.Amount,
				"from", created.State, "to", state)

			ignores[created.FromAccountID] = struct{}{}
		}

		offset += limit
	}
}

func getAddress(ctx context.Context, id string) (string, error) {
	acc, err := accountmwcli.GetAccount(ctx, id)
	if err != nil {
		return "", err
	}
	if acc == nil {
		return "", fmt.Errorf("invalid account")
	}

	return acc.Address, nil
}

func getMemo(ctx context.Context, tx *txmwpb.Tx, id string) (string, error) {
	acc, err := useraccmwcli.GetAccountOnly(ctx, &useraccmwpb.Conds{
		AccountID:  &commonpb.StringVal{Op: cruder.EQ, Value: id},
		CoinTypeID: &commonpb.StringVal{Op: cruder.EQ, Value: tx.CoinTypeID},
		Active:     &commonpb.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:    &commonpb.BoolVal{Op: cruder.EQ, Value: false},
		UsedFor:    &commonpb.Int32Val{Op: cruder.EQ, Value: int32(basetypes.UsedFor_Withdraw)},
	})
	if err != nil {
		return "", err
	}
	if acc == nil {
		return "", fmt.Errorf("invalid user account")
	}
	return acc.Memo, nil
}

func transfer(ctx context.Context, tx *txmwpb.Tx) error {
	logger.Sugar().Infow("transaction", "id", tx.ID, "amount", tx.Amount, "state", tx.State)

	fromAddress, err := getAddress(ctx, tx.FromAccountID)
	if err != nil {
		logger.Sugar().Errorw("transaction", "Account", tx.FromAccountID, "error", err)
		return err
	}
	toAddress, err := getAddress(ctx, tx.ToAccountID)
	if err != nil {
		logger.Sugar().Errorw("transaction", "Account", tx.ToAccountID, "error", err)
		return err
	}
	var memo *string
	if tx.Type == basetypes.TxType_TxWithdraw {
		_memo, err := getMemo(ctx, tx, tx.ToAccountID)
		if err != nil {
			logger.Sugar().Errorw("transaction", "Account", tx.ToAccountID, "error", err)
			return err
		}
		if _memo != "" {
			memo = &_memo
		}
	}

	coin, err := coinmwcli.GetCoin(ctx, tx.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	logger.Sugar().Infow("transaction", "id", tx.ID,
		"coin", coin.Name, "from", fromAddress, "to", toAddress,
		"amount", tx.Amount, "fee", tx.FeeAmount)

	amount, err := decimal.NewFromString(tx.Amount)
	if err != nil {
		return err
	}

	feeAmount, err := decimal.NewFromString(tx.FeeAmount)
	if err != nil {
		return err
	}

	amount = amount.Sub(feeAmount)
	transferAmount := amount.InexactFloat64()

	err = sphinxproxycli.CreateTransaction(ctx, &sphinxproxypb.CreateTransactionRequest{
		TransactionID: tx.ID,
		Name:          coin.Name,
		Amount:        transferAmount,
		From:          fromAddress,
		Memo:          memo,
		To:            toAddress,
	})
	if err != nil {
		return err
	}

	return nil
}

func onWaitChecker(ctx context.Context) {
	offset := int32(0)
	const limit = int32(1000)

	for {
		waits, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
			State: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(txmgrpb.TxState_StateWait),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("onWaitChecker", "error", err)
			return
		}
		if len(waits) == 0 {
			return
		}

		for _, wait := range waits {
			tx, _ := sphinxproxycli.GetTransaction(ctx, wait.ID) //nolint
			if tx != nil {
				state := txmgrpb.TxState_StateTransferring
				_, err := txmwcli.UpdateTx(ctx, &txmgrpb.TxReq{
					ID:    &wait.ID,
					State: &state,
				})
				if err != nil {
					logger.Sugar().Errorw("onWaitChecker", "id", wait.ID, "error", err)
					return
				}
				continue
			}

			if err := transfer(ctx, wait); err != nil {
				logger.Sugar().Errorw("onWaitChecker", "id", wait.ID, "error", err)
				continue
			}

			state := txmgrpb.TxState_StateTransferring
			_, err := txmwcli.UpdateTx(ctx, &txmgrpb.TxReq{
				ID:    &wait.ID,
				State: &state,
			})
			if err != nil {
				logger.Sugar().Errorw("onWaitChecker", "id", wait.ID, "error", err)
				return
			}
		}
		offset += limit
	}
}

func onPayingChecker(ctx context.Context) { //nolint
	offset := int32(0)
	const limit = int32(1000)

	for {
		payings, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
			State: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(txmgrpb.TxState_StateTransferring),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("onPayingChecker", "error", err)
			return
		}
		if len(payings) == 0 {
			return
		}

		for _, paying := range payings {
			toState := txmgrpb.TxState_StateTransferring
			cid := ""

			tx, err := sphinxproxycli.GetTransaction(ctx, paying.ID)
			if err != nil {
				logger.Sugar().Errorw("onPayingChecker", "id", paying.ID, "error", err)
				switch status.Code(err) {
				case codes.InvalidArgument:
					fallthrough //nolint
				case codes.NotFound:
					toState = txmgrpb.TxState_StateFail
				default:
					continue
				}
			} else if tx == nil {
				logger.Sugar().Errorw("transaction", "id", paying.ID, "error", "invalid transaction id")
				continue
			}

			extra := ""

			if toState == txmgrpb.TxState_StateTransferring {
				switch tx.TransactionState {
				case sphinxproxypb.TransactionState_TransactionStateFail:
					toState = txmgrpb.TxState_StateFail
				case sphinxproxypb.TransactionState_TransactionStateDone:
					toState = txmgrpb.TxState_StateSuccessful
					if tx.CID == "" {
						extra = "(successful without CID)"
						toState = txmgrpb.TxState_StateFail
					}
					cid = tx.CID
				default:
					continue
				}
			}

			_, err = txmwcli.UpdateTx(ctx, &txmgrpb.TxReq{
				ID:        &paying.ID,
				ChainTxID: &cid,
				State:     &toState,
				Extra:     &extra,
			})
			if err != nil {
				logger.Sugar().Errorw("onPayingChecker", "id", paying.ID, "error", err)
				return
			}
		}

		offset += limit
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			onCreatedChecker(ctx)
			onWaitChecker(ctx)
			onPayingChecker(ctx)
		case <-ctx.Done():
			return
		}
	}
}
