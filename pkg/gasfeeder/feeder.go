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

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	"github.com/shopspring/decimal"
)

func account(ctx context.Context, coinTypeID string, usedFor accountmgrpb.AccountUsedFor) (*pltfaccmwpb.Account, error) {
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
		return nil, err
	}
	if acc == nil {
		return nil, fmt.Errorf("invalid account")
	}
	if acc.Address == "" {
		return nil, fmt.Errorf("%v invalid %v account", coinTypeID, usedFor)
	}
	return acc, nil
}

func feeding(ctx context.Context, accountID string) (bool, error) {
	txs, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
		AccountID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: accountID,
		},
		Type: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(basetypes.TxType_TxFeedGas),
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
		return false, fmt.Errorf("%v [%v] balance error: %v", coinName, address, err)
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
	usedFor accountmgrpb.AccountUsedFor,
	amount decimal.Decimal,
	lowFeeAmount decimal.Decimal,
) (
	bool, error,
) {
	if address == "" || accountID == "" {
		return false, fmt.Errorf("coin %v account %v address %v usedFor %v", coin.Name, accountID, address, usedFor)
	}

	ok, err := enough(ctx, feeCoin.Name, address, lowFeeAmount)
	if err != nil {
		return false, fmt.Errorf("target account %v error: %v", accountID, err)
	}
	if ok {
		logger.Sugar().Infow("feedOne",
			"Coin", coin.Name,
			"feeCoin", feeCoin.Name,
			"Address", address,
			"LowFeeAmount", lowFeeAmount,
			"Enough", ok,
		)
		return false, nil
	}

	yes, err := feeding(ctx, accountID)
	if err != nil {
		return false, err
	}
	if yes {
		logger.Sugar().Infow("feedOne",
			"Coin", coin.Name,
			"feeCoin", feeCoin.Name,
			"AccountID", accountID,
			"Feeding", true,
		)
		return true, nil
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: address,
	})
	if err != nil {
		return false, fmt.Errorf("%v [%v:%v] balance error: %v", coin.Name, accountID, address, err)
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
		logger.Sugar().Infow("feedOne",
			"Coin", coin.Name,
			"feeCoin", feeCoin.Name,
			"Address", address,
			"Balance", bal,
			"ReservedAmount", reserved,
		)
		return false, nil
	}

	ok, err = enough(ctx, feeCoin.Name, gasProvider.Address, amount)
	if err != nil {
		return false, fmt.Errorf("provider account %v error: %v", gasProvider.AccountID, err)
	}
	if !ok {
		return false, fmt.Errorf("insufficient funds")
	}

	amountS := amount.String()
	feeAmountS := "0"
	txType := basetypes.TxType_TxFeedGas
	txExtra := fmt.Sprintf(`{"Coin":"%v","AccountType":"%v","FeeCoinTypeID":"%v"}`,
		coin.Name, usedFor, coin.FeeCoinTypeID)

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
		return false, fmt.Errorf("%v benefit hot error: %v", coin.Name, err)
	}
	if acc == nil {
		return false, fmt.Errorf("invalid hot account")
	}

	amount, err := decimal.NewFromString(coin.HotWalletFeeAmount)
	if err != nil {
		return false, err
	}

	lowFeeAmount, err := decimal.NewFromString(coin.HotLowFeeAmount)
	if err != nil {
		return false, fmt.Errorf("coin %v lowFeeAmount %v err %v", coin.Name, coin.HotLowFeeAmount, err)
	}

	return feedOne(ctx, coin, feeCoin, gasProvider, acc.AccountID, acc.Address, acc.UsedFor, amount, lowFeeAmount)
}

func feedPaymentAccount(ctx context.Context, coin, feeCoin *coinmwpb.Coin, gasProvider *pltfaccmwpb.Account) (bool, error) { //nolint
	offset := int32(0)
	const limit = int32(100)

	amount, err := decimal.NewFromString(coin.CollectFeeAmount)
	if err != nil {
		return false, err
	}

	lowFeeAmount, err := decimal.NewFromString(coin.LowFeeAmount)
	if err != nil {
		return false, fmt.Errorf("coin %v lowFeeAmount %v err %v", coin.Name, coin.LowFeeAmount, err)
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
			feeded, err := feedOne(
				ctx,
				coin,
				feeCoin,
				gasProvider,
				acc.AccountID,
				acc.Address,
				accountmgrpb.AccountUsedFor_GoodPayment,
				amount,
				lowFeeAmount,
			)
			if err != nil {
				continue
			}
			if feeded {
				return true, nil
			}
		}

		offset += limit
	}
}

func feedDepositAccount(ctx context.Context, coin, feeCoin *coinmwpb.Coin, gasProvider *pltfaccmwpb.Account) (bool, error) { //nolint
	offset := int32(0)
	const limit = int32(100)

	amount, err := decimal.NewFromString(coin.CollectFeeAmount)
	if err != nil {
		return false, err
	}

	lowFeeAmount, err := decimal.NewFromString(coin.LowFeeAmount)
	if err != nil {
		return false, fmt.Errorf("coin %v lowFeeAmount %v err %v", coin.Name, coin.LowFeeAmount, err)
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

		logger.Sugar().Infow(
			"feedDespositAccount",
			"Accounts", len(accs),
			"Coin", coin.Name,
			"CoinTypeID", coin.ID,
			"FeeCoin", feeCoin.Name,
			"FeeCoinTypeID", feeCoin.ID,
		)

		for _, acc := range accs {
			feeded, err := feedOne(
				ctx,
				coin,
				feeCoin,
				gasProvider,
				acc.AccountID,
				acc.Address,
				accountmgrpb.AccountUsedFor_UserDeposit,
				amount,
				lowFeeAmount,
			)
			if err != nil {
				continue
			}
			if feeded {
				return true, nil
			}
		}

		offset += limit
	}
}

func feedGoodBenefitAccount(ctx context.Context, coin, feeCoin *coinmwpb.Coin, gasProvider *pltfaccmwpb.Account) (bool, error) {
	return false, nil
}

func feedCoin(ctx context.Context, coin *coinmwpb.Coin) error {
	acc, err := account(ctx, coin.FeeCoinTypeID, accountmgrpb.AccountUsedFor_GasProvider)
	if err != nil {
		return fmt.Errorf("%v [%v] gas provider error: %v", coin.Name, coin.FeeCoinName, err)
	}
	if acc == nil {
		return fmt.Errorf("invalid gas provider account")
	}

	yes, err := feeding(ctx, acc.AccountID)
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

	feeded, err = feedDepositAccount(ctx, coin, feeCoin, acc)
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

		logger.Sugar().Infow("gasfeeder", "FeedGas", "Start...")
		for {
			coins, _, err := coinmwcli.GetCoins(ctx, &coinmwpb.Conds{}, offset, limit)
			if err != nil {
				logger.Sugar().Errorw("gasfeeder", "Offset", offset, "Limit", limit)
				break
			}
			if len(coins) == 0 {
				break
			}

			for _, coin := range coins {
				if coin.FeeCoinTypeID == uuid1.InvalidUUIDStr || coin.FeeCoinTypeID == "" {
					logger.Sugar().Warnw(
						"gasfeeder",
						"Coin", coin.Name,
						"CoinTypeID", coin.ID,
						"FeeCoinType", coin.FeeCoinTypeID,
						"State", "Empty",
					)
					continue
				}

				if coin.ID == coin.FeeCoinTypeID {
					logger.Sugar().Warnw(
						"gasfeeder",
						"Coin", coin.Name,
						"CoinTypeID", coin.ID,
						"FeeCoinType", coin.FeeCoinTypeID,
						"State", "Equal",
					)
					continue
				}

				logger.Sugar().Warnw(
					"gasfeeder",
					"Coin", coin.Name,
					"CoinTypeID", coin.ID,
					"FeeCoin", coin.FeeCoinName,
				)
				if err := feedCoin(ctx, coin); err != nil {
					logger.Sugar().Errorw("gasfeeder", "Coin", coin.Name, "error", err)
				}
			}

			offset += limit
		}

		logger.Sugar().Infow("gasfeeder", "FeedGas", "End...")
	}
}
