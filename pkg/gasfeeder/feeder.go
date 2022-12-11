package gasfeeder

import (
	"context"
	"fmt"
	"time"

	uuid1 "github.com/NpoolPlatform/go-service-framework/pkg/const/uuid"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"

	depositaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	commonpb "github.com/NpoolPlatform/message/npool"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	"github.com/shopspring/decimal"
)

func account(ctx context.Context, coinTypeID string, usedFor accountmgrpb.AccountUsedFor) (*pltfaccmwpb.Account, error) {
	return pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
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
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
	})
}

func feeding(ctx context.Context, accountID string) (bool, error) {
	txs, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
		AccountID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: accountID,
		},
		Type: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(txmgrpb.TxType_TxFeedGas),
		},
	}, int32(0), int32(1)) //nolint
	if err != nil {
		return true, err
	}
	if len(txs) == 0 {
		return false, nil
	}

	switch txs[0].State {
	case txmgrpb.TxState_StateCreated:
		fallthrough //nolint
	case txmgrpb.TxState_StateWait:
		fallthrough //nolint
	case txmgrpb.TxState_StateTransferring:
		return true, nil
	case txmgrpb.TxState_StateSuccessful:
	case txmgrpb.TxState_StateFail:
		return false, nil
	}

	const coolDown = uint32(10 * 60)
	if txs[0].UpdatedAt+coolDown > uint32(time.Now().Unix()) {
		return true, nil
	}

	return false, nil
}

func enough(ctx context.Context, coinName, address string, amount decimal.Decimal) (bool, error) {
	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coinName,
		Address: address,
	})
	if err != nil {
		return false, err
	}
	if balance == nil {
		return false, fmt.Errorf("invalid balance")
	}

	bal, err := decimal.NewFromString(balance.BalanceStr)
	if err != nil {
		return false, err
	}

	if bal.Cmp(amount) <= 0 {
		return false, nil
	}

	return true, nil
}

func feedOne(
	ctx context.Context,
	coin, feeCoin *coinmwpb.Coin,
	gasProvider *pltfaccmwpb.Account,
	accountID, address string,
	amount decimal.Decimal,
) (
	bool, error,
) {
	ok, err := enough(ctx, feeCoin.Name, address, amount)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}

	yes, err := feeding(ctx, accountID)
	if err != nil {
		return false, err
	}
	if yes {
		return true, nil
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: address,
	})
	if err != nil {
		return false, err
	}
	if balance == nil {
		return false, fmt.Errorf("invalid balance")
	}

	bal, err := decimal.NewFromString(balance.BalanceStr)
	if err != nil {
		return false, err
	}

	reserved, err := decimal.NewFromString(coin.ReservedAmount)
	if err != nil {
		return false, err
	}

	if bal.Cmp(reserved) <= 0 {
		return false, nil
	}

	ok, err = enough(ctx, feeCoin.Name, gasProvider.Address, amount)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("insufficient funds")
	}

	amountS := amount.String()
	feeAmountS := "0"
	txType := txmgrpb.TxType_TxFeedGas
	txExtra := fmt.Sprintf(`{"Coin":"%v","AccountType":"%v","FeeCoinTypeID":"%v"}`,
		coin.Name, accountmgrpb.AccountUsedFor_UserBenefitHot, coin.FeeCoinTypeID)

	_, err = txmwcli.CreateTx(ctx, &txmgrpb.TxReq{
		CoinTypeID:    &coin.FeeCoinTypeID,
		FromAccountID: &gasProvider.AccountID,
		ToAccountID:   &accountID,
		Amount:        &amountS,
		FeeAmount:     &feeAmountS,
		Extra:         &txExtra,
		Type:          &txType,
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

func feedUserBenefitHotAccount(ctx context.Context, coin, feeCoin *coinmwpb.Coin, gasProvider *pltfaccmwpb.Account) (bool, error) {
	acc, err := account(ctx, coin.ID, accountmgrpb.AccountUsedFor_UserBenefitHot)
	if err != nil {
		return false, err
	}
	if acc == nil {
		return false, fmt.Errorf("invalid hot account")
	}

	amount, err := decimal.NewFromString(coin.HotWalletFeeAmount)
	if err != nil {
		return false, err
	}
	return feedOne(ctx, coin, feeCoin, gasProvider, acc.ID, acc.Address, amount)
}

func feedPaymentAccount(ctx context.Context, coin, feeCoin *coinmwpb.Coin, gasProvider *pltfaccmwpb.Account) (bool, error) { //nolint
	offset := int32(0)
	const limit = int32(100)

	amount, err := decimal.NewFromString(coin.CollectFeeAmount)
	if err != nil {
		return false, err
	}

	for {
		accs, _, err := payaccmwcli.GetAccounts(ctx, &payaccmwpb.Conds{
			CoinTypeID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: coin.ID,
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
		}, offset, limit)
		if err != nil {
			return false, err
		}
		if len(accs) == 0 {
			return false, nil
		}

		for _, acc := range accs {
			feeded, err := feedOne(ctx, coin, feeCoin, gasProvider, acc.ID, acc.Address, amount)
			if err != nil {
				return false, err
			}
			if feeded {
				return true, nil
			}
		}
	}
}

func feedDepositAccount(ctx context.Context, coin, feeCoin *coinmwpb.Coin, gasProvider *pltfaccmwpb.Account) (bool, error) { //nolint
	offset := int32(0)
	const limit = int32(100)

	amount, err := decimal.NewFromString(coin.CollectFeeAmount)
	if err != nil {
		return false, err
	}

	for {
		accs, _, err := depositaccmwcli.GetAccounts(ctx, &depositaccmwpb.Conds{
			CoinTypeID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: coin.ID,
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
		}, offset, limit)
		if err != nil {
			return false, err
		}
		if len(accs) == 0 {
			return false, nil
		}

		for _, acc := range accs {
			feeded, err := feedOne(ctx, coin, feeCoin, gasProvider, acc.ID, acc.Address, amount)
			if err != nil {
				return false, err
			}
			if feeded {
				return true, nil
			}
		}
	}
}

func feedGoodBenefitAccount(ctx context.Context, coin, feeCoin *coinmwpb.Coin, gasProvider *pltfaccmwpb.Account) (bool, error) {
	return false, nil
}

func feedCoin(ctx context.Context, coin *coinmwpb.Coin) error {
	acc, err := account(ctx, coin.FeeCoinTypeID, accountmgrpb.AccountUsedFor_GasProvider)
	if err != nil {
		return err
	}
	if acc == nil {
		return fmt.Errorf("invalid gas provider account")
	}

	yes, err := feeding(ctx, acc.ID)
	if err != nil {
		return err
	}
	if yes {
		return nil
	}

	feeCoin, err := coinmwcli.GetCoin(ctx, coin.FeeCoinTypeID)
	if err != nil {
		return err
	}

	feeded, err := feedUserBenefitHotAccount(ctx, coin, feeCoin, acc)
	if err != nil {
		return err
	}
	if feeded {
		return nil
	}

	feeded, err = feedPaymentAccount(ctx, coin, feeCoin, acc)
	if err != nil {
		return err
	}
	if feeded {
		return nil
	}

	feeded, err = feedDepositAccount(ctx, coin, feeCoin, acc)
	if err != nil {
		return err
	}
	if feeded {
		return nil
	}

	_, err = feedGoodBenefitAccount(ctx, coin, feeCoin, acc)
	if err != nil {
		return err
	}

	return nil
}

func Watch(ctx context.Context) { //nolint
	ticker := time.NewTicker(time.Minute)

	for range ticker.C {
		offset := int32(0)
		const limit = int32(100)

		for {
			coins, _, err := coinmwcli.GetCoins(ctx, &coinmwpb.Conds{}, offset, limit)
			if err != nil {
				break
			}
			if len(coins) == 0 {
				break
			}

			for _, coin := range coins {
				if coin.FeeCoinTypeID == uuid1.InvalidUUIDStr || coin.FeeCoinTypeID == "" {
					continue
				}

				if coin.ID == coin.FeeCoinTypeID {
					continue
				}

				if err := feedCoin(ctx, coin); err != nil {
					logger.Sugar().Errorw("gasfeeder", "Coin", coin.Name, "error", err)
				}
			}

			offset += limit
		}
	}
}
