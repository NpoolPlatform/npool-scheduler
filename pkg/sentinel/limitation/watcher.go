package limitation

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"

	commonpb "github.com/NpoolPlatform/message/npool"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

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
		Locked: &commonpb.BoolVal{
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
	})
	if err != nil {
		return nil, err
	}
	if acc == nil {
		return nil, fmt.Errorf("invalid account")
	}

	return acc, nil
}

func accounts(ctx context.Context, coinTypeID string) (hot, cold *pltfaccmwpb.Account, err error) {
	hot, err = account(ctx, coinTypeID, accountmgrpb.AccountUsedFor_UserBenefitHot)
	if err != nil {
		return nil, nil, err
	}

	cold, err = account(ctx, coinTypeID, accountmgrpb.AccountUsedFor_UserBenefitCold)
	if err != nil {
		return nil, nil, err
	}

	return hot, cold, nil
}

func transaction(ctx context.Context, account *pltfaccmwpb.Account, state txmgrpb.TxState) (*txmwpb.Tx, error) {
	txs, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: account.CoinTypeID,
		},
		AccountID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: account.AccountID,
		},
		State: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(state),
		},
		Type: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(basetypes.TxType_TxLimitation),
		},
	}, int32(0), int32(1)) //nolint
	if err != nil {
		return nil, err
	}
	if len(txs) == 0 {
		return nil, nil
	}

	return txs[0], nil
}

func transferring(ctx context.Context, account *pltfaccmwpb.Account) (bool, error) {
	tx, err := transaction(ctx, account, txmgrpb.TxState_StateCreated)
	if err != nil {
		return true, err
	}
	if tx != nil {
		return true, nil
	}

	tx, err = transaction(ctx, account, txmgrpb.TxState_StateWait)
	if err != nil {
		return true, err
	}
	if tx != nil {
		return true, nil
	}

	tx, err = transaction(ctx, account, txmgrpb.TxState_StateTransferring)
	if err != nil {
		return true, err
	}
	if tx != nil {
		return true, nil
	}

	const accountCoollDownSeconds = 1 * 60 * 60

	tx, err = transaction(ctx, account, txmgrpb.TxState_StateSuccessful)
	if err != nil {
		return true, err
	}
	if tx != nil {
		if tx.CreatedAt+accountCoollDownSeconds > uint32(time.Now().Unix()) {
			return true, nil
		}
	}

	return false, nil
}

func checkCoinLimit(ctx context.Context, coin *coinmwpb.Coin) error {
	online, offline, err := accounts(ctx, coin.ID)
	if err != nil {
		return err
	}

	limit, err := decimal.NewFromString(coin.HotWalletAccountAmount)
	if err != nil {
		logger.Sugar().Errorw(
			"checkCoinLimit",
			"coin", coin.Name,
			"amount", coin.HotWalletAccountAmount,
			"error", err,
		)
		return err
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: online.Address,
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

	if bal.Cmp(limit.Mul(decimal.NewFromInt(2))) > 0 {
		return nil
	}

	yes, err := transferring(ctx, offline)
	if err != nil {
		return err
	}
	if yes {
		return nil
	}

	amountS := bal.Sub(limit).String()
	feeAmountS := "0"
	txType := basetypes.TxType_TxLimitation

	_, err = txmwcli.CreateTx(ctx, &txmgrpb.TxReq{
		CoinTypeID:    &coin.ID,
		FromAccountID: &online.AccountID,
		ToAccountID:   &offline.AccountID,
		Amount:        &amountS,
		FeeAmount:     &feeAmountS,
		Type:          &txType,
	})

	return err
}

func checkCoinLimits(ctx context.Context) {
	offset := int32(0)
	const limit = int32(100)

	for {
		coins, _, err := coinmwcli.GetCoins(ctx, &coinmwpb.Conds{}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("checkCoinLimits", "error", err)
			return
		}
		if len(coins) == 0 {
			return
		}

		for _, coin := range coins {
			if err := checkCoinLimit(ctx, coin); err != nil {
				logger.Sugar().Errorw("checkCoinLimits", "error", err)
			}
		}

		offset += limit
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(4 * time.Hour)

	for {
		checkCoinLimits(ctx)
		<-ticker.C
	}
}
