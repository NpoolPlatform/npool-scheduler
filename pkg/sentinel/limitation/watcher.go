package limitation

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	currency "github.com/NpoolPlatform/oracle-manager/pkg/middleware/currency"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	"github.com/google/uuid"
)

const (
	defaultLimitAmount      = 10000.0
	leastLimitAmount        = 0.1
	accountCoollDownSeconds = 1 * 60 * 60
)

func coinLimit(ctx context.Context, coin *coininfopb.CoinInfo, setting *billingpb.CoinSetting) (float64, error) {
	limit := 0.0

	if setting != nil {
		// TODO: use decimal for amount
		limit = setting.WarmAccountCoinAmount
	}

	if limit == 0 {
		psetting, err := billingcli.GetPlatformSetting(ctx)
		if err != nil {
			return defaultLimitAmount, err
		}

		price, err := currency.USDPrice(ctx, coin.Name)
		if err != nil {
			return defaultLimitAmount, err
		}

		limit = psetting.WarmAccountUSDAmount / price
	}

	if limit < leastLimitAmount {
		return leastLimitAmount, nil
	}

	return limit, nil
}

func accounts(ctx context.Context, setting *billingpb.CoinSetting) (online, offline *billingpb.CoinAccountInfo, err error) {
	if setting == nil {
		return nil, nil, fmt.Errorf("invalid coin setting")
	}

	online, err = billingcli.GetAccount(ctx, setting.UserOnlineAccountID)
	if err != nil {
		return nil, nil, err
	}
	if online == nil {
		return nil, nil, fmt.Errorf("invalid online account")
	}

	offline, err = billingcli.GetAccount(ctx, setting.UserOfflineAccountID)
	if err != nil {
		return nil, nil, err
	}
	if offline == nil {
		return nil, nil, fmt.Errorf("invalid offline account")
	}

	return online, offline, err
}

func transferring(ctx context.Context, coin *coininfopb.CoinInfo, setting *billingpb.CoinSetting) (bool, error) {
	if setting == nil {
		return false, fmt.Errorf("invalid coin setting")
	}

	txs, err := billingcli.GetAccountTransactions(ctx, coin.ID, setting.UserOfflineAccountID)
	if err != nil {
		return false, err
	}

	for _, tx := range txs {
		switch tx.State {
		case billingconst.CoinTransactionStateCreated:
			fallthrough //nolint
		case billingconst.CoinTransactionStateWait:
			fallthrough //nolint
		case billingconst.CoinTransactionStatePaying:
			return true, nil
		case billingconst.CoinTransactionStateSuccessful:
			if tx.CreateAt+accountCoollDownSeconds > uint32(time.Now().Unix()) {
				return true, nil
			}
		default:
		}
	}

	return false, nil
}

func checkCoinLimit(ctx context.Context, coin *coininfopb.CoinInfo) error {
	csetting, err := billingcli.GetCoinSetting(ctx, coin.ID)
	if err != nil {
		return err
	}

	online, _, err := accounts(ctx, csetting)
	if err != nil {
		return err
	}

	limit, err := coinLimit(ctx, coin, csetting)
	if err != nil {
		// Here we do not return, just use default limitation
		logger.Sugar().Errorw("checkCoinLimit", "coin", coin.Name, "id", coin.ID, "error", err)
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: online.Address,
	})
	if err != nil {
		return err
	}

	// TODO: use decimal other than float
	if balance.Balance < limit*2 {
		return nil
	}

	yes, err := transferring(ctx, coin, csetting)
	if yes {
		return nil
	}
	if err != nil {
		return err
	}

	_, err = billingcli.CreateTransaction(ctx, &billingpb.CoinAccountTransaction{
		AppID:              uuid.UUID{}.String(),
		UserID:             uuid.UUID{}.String(),
		GoodID:             uuid.UUID{}.String(),
		FromAddressID:      csetting.UserOnlineAccountID,
		ToAddressID:        csetting.UserOfflineAccountID,
		CoinTypeID:         coin.ID,
		Amount:             balance.Balance - limit,
		Message:            fmt.Sprintf("warm transfer at %v", time.Now()),
		ChainTransactionID: "",
		CreatedFor:         billingconst.TransactionForWarmTransfer,
	})
	return err
}

func checkCoinLimits(ctx context.Context) {
	coins, err := coininfocli.GetCoinInfos(ctx, cruder.NewFilterConds())
	if err != nil {
		logger.Sugar().Errorw("checkCoinLimits", "error", err)
		return
	}

	for _, coin := range coins {
		if err := checkCoinLimit(ctx, coin); err != nil {
			logger.Sugar().Errorw("checkCoinLimits", "error", err)
		}
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(4 * time.Hour)

	for {
		checkCoinLimits(ctx)
		<-ticker.C
	}
}
