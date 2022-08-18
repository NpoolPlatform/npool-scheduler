package deposit

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	depositmgrcli "github.com/NpoolPlatform/account-manager/pkg/client/deposit"
	depositmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	depositmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/deposit"
	depositmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	commonpb "github.com/NpoolPlatform/message/npool"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	"github.com/shopspring/decimal"
)

func depositOne(ctx context.Context, acc *depositmwpb.Account) error {
	if acc.Locked {
		return nil
	}

	incoming, err := decimal.NewFromString(acc.Incoming)
	if err != nil {
		return nil
	}

	outcoming, err := decimal.NewFromString(acc.Outcoming)
	if err != nil {
		return nil
	}

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

	_, err = depositmgrcli.AddAccount(ctx, &depositmgrpb.AccountReq{
		ID:         &acc.ID,
		AppID:      &acc.AppID,
		UserID:     &acc.UserID,
		CoinTypeID: &acc.CoinTypeID,
		AccountID:  &acc.AccountID,
		Incoming:   &amount,
	})
	if err != nil {
		return err
	}

	ioExtra := fmt.Sprintf(`{"AccountID": "%v"}`, acc.AccountID)
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

	for {
		accs, err := depositmwcli.GetAccounts(ctx, &depositmwpb.Conds{
			ScannableAt: &commonpb.Uint32Val{
				Op:    cruder.GT,
				Value: uint32(time.Now().Unix()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("deposit", "error", err)
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

func transfer(ctx context.Context) {
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)

	for {
		select {
		case <-ticker.C:
			deposit(ctx)
			transfer(ctx)
		case <-ctx.Done():
			return
		}
	}
}
