//nolint:dupl
package deposit

import (
	"context"
	"fmt"
	"strings"
	"time"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	depositmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	uuid1 "github.com/NpoolPlatform/go-service-framework/pkg/const/uuid"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	depositmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"

	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	tmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"

	watcher "github.com/NpoolPlatform/staker-manager/pkg/watcher"

	"github.com/shopspring/decimal"
)

func depositOne(ctx context.Context, acc *depositmwpb.Account) error {
	if acc.Locked {
		return nil
	}

	incoming, _ := decimal.NewFromString(acc.Incoming)   //nolint
	outcoming, _ := decimal.NewFromString(acc.Outcoming) //nolint

	coin, err := coinmwcli.GetCoin(ctx, acc.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	logger.Sugar().Infow(
		"depositOne",
		"AccountID", acc.AccountID,
		"Address", acc.Address,
		"State", "Lock",
	)
	if err := accountlock.Lock(acc.AccountID); err != nil {
		return err
	}
	defer func() {
		logger.Sugar().Infow(
			"depositOne",
			"AccountID", acc.AccountID,
			"Address", acc.Address,
			"State", "Unlock",
		)
		_ = accountlock.Unlock(acc.AccountID) //nolint
	}()

	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: acc.Address,
	})
	if err != nil {
		return fmt.Errorf("fail get balance coin %v address %v error %v", coin.Name, acc.Address, err)
	}
	if bal == nil {
		return fmt.Errorf("fail get balance coin %v address %v", coin.Name, acc.Address)
	}

	balance, err := decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}

	if balance.Cmp(incoming.Sub(outcoming)) <= 0 {
		return nil
	}

	amount := balance.Sub(incoming.Sub(outcoming)).String()

	// TODO: move add and book keeping to TX

	_, err = depositmwcli.UpdateAccount(ctx, &depositmwpb.AccountReq{
		ID:        &acc.ID,
		AppID:     &acc.AppID,
		UserID:    &acc.UserID,
		AccountID: &acc.AccountID,
		Incoming:  &amount,
	})
	if err != nil {
		return err
	}

	ioExtra := fmt.Sprintf(
		`{"AppID":"%v","UserID":"%v","AccountID":"%v","CoinName":"%v","Address":"%v","Date":"%v"}`,
		acc.AppID,
		acc.UserID,
		acc.AccountID,
		coin.Name,
		acc.Address,
		time.Now(),
	)
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Deposit

	err = ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &acc.AppID,
		UserID:     &acc.UserID,
		CoinTypeID: &acc.CoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
	if err != nil {
		return err
	}

	username := ""

	user, err := usermwcli.GetUser(ctx, acc.AppID, acc.UserID)
	if err == nil && user != nil {
		username = user.Username
	}

	now := uint32(time.Now().Unix())

	_, err = notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
		AppID:     acc.AppID,
		UserID:    acc.UserID,
		EventType: basetypes.UsedFor_DepositReceived,
		NotifType: basetypes.NotifType_NotifUnicast,
		Vars: &tmplmwpb.TemplateVars{
			Username:  &username,
			Amount:    &amount,
			CoinUnit:  &coin.Unit,
			Address:   &acc.Address,
			Timestamp: &now,
		},
	})
	if err != nil {
		logger.Sugar().Errorw("depositOne", "Error", err)
	}

	return nil
}

func deposit(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	logger.Sugar().Infow("deposit", "Start", "...")

	for {
		accs, _, err := depositmwcli.GetAccounts(ctx, &depositmwpb.Conds{
			Locked:      &basetypes.BoolVal{Op: cruder.EQ, Value: false},
			ScannableAt: &basetypes.Uint32Val{Op: cruder.LT, Value: uint32(time.Now().Unix())},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("deposit", "error", err)
			return
		}
		if len(accs) == 0 {
			logger.Sugar().Infow("deposit", "Done", "...")
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

func tryTransferOne(ctx context.Context, acc *depositmwpb.Account) error { //nolint
	if acc.Locked {
		return nil
	}

	logger.Sugar().Errorw(
		"tryTransferOne",
		"Account", acc.Address,
		"CoinTypeID", acc.CoinTypeID,
		"CollectingTID", acc.CollectingTID,
	)

	tx, err := txmwcli.GetTx(ctx, acc.CollectingTID)
	if err != nil && !strings.Contains(err.Error(), "no record") {
		logger.Sugar().Errorw(
			"tryTransferOne",
			"Account", acc.Address,
			"CoinTypeID", acc.CoinTypeID,
			"CollectingTID", acc.CollectingTID,
			"error", err,
		)
		return err
	}
	if tx != nil {
		switch tx.State {
		case basetypes.TxState_TxStateSuccessful:
		case basetypes.TxState_TxStateFail:
		default:
			return nil
		}
	}

	incoming, _ := decimal.NewFromString(acc.Incoming)   //nolint
	outcoming, _ := decimal.NewFromString(acc.Outcoming) //nolint

	coin, err := coinmwcli.GetCoin(ctx, acc.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	limit, err := decimal.NewFromString(coin.PaymentAccountCollectAmount)
	if err != nil {
		logger.Sugar().Errorw(
			"tryTransferOne",
			"Coin", coin.Name,
			"Threshold", coin.PaymentAccountCollectAmount,
			"error", err,
		)
		return err
	}

	if incoming.Sub(outcoming).Cmp(limit) < 0 {
		return nil
	}

	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: acc.Address,
	})
	if err != nil {
		return fmt.Errorf("fail get balance coin %v address %v error %v", coin.Name, acc.Address, err)
	}
	if bal == nil {
		return fmt.Errorf("fail get balance coin %v address %v", coin.Name, acc.Address)
	}

	balance, err := decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}

	reserved, err := decimal.NewFromString(coin.ReservedAmount)
	if err != nil {
		return err
	}

	if balance.Cmp(incoming.Sub(outcoming)) < 0 {
		return fmt.Errorf("insufficient funds")
	}
	if incoming.Sub(outcoming).Cmp(reserved) <= 0 {
		return nil
	}

	collect, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: coin.ID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountUsedFor_PaymentCollector)},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return nil
	}
	if collect == nil {
		return fmt.Errorf("invalid collect account")
	}

	if err := accountlock.Lock(acc.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(acc.AccountID) //nolint
	}()

	amountS := incoming.Sub(outcoming).Sub(reserved).String()
	feeAmountS := "0"
	txType := basetypes.TxType_TxPaymentCollect

	// TODO: reliable record collecting TID

	tx, err = txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		CoinTypeID:    &coin.ID,
		FromAccountID: &acc.AccountID,
		ToAccountID:   &collect.AccountID,
		Amount:        &amountS,
		FeeAmount:     &feeAmountS,
		Type:          &txType,
	})
	if err != nil {
		return err
	}

	locked := true
	lockedBy := basetypes.AccountLockedBy_Collecting

	_, err = depositmwcli.UpdateAccount(ctx, &depositmwpb.AccountReq{
		ID:            &acc.ID,
		AppID:         &acc.AppID,
		UserID:        &acc.UserID,
		CoinTypeID:    &acc.CoinTypeID,
		AccountID:     &acc.AccountID,
		Locked:        &locked,
		LockedBy:      &lockedBy,
		CollectingTID: &tx.ID,
	})
	if err != nil {
		return err
	}

	return nil
}

func transfer(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	logger.Sugar().Infow("transfer", "Start", "...")

	for {
		accs, _, err := depositmwcli.GetAccounts(ctx, &depositmwpb.Conds{
			Locked: &basetypes.BoolVal{
				Op:    cruder.EQ,
				Value: false,
			},
			ScannableAt: &basetypes.Uint32Val{
				Op:    cruder.LT,
				Value: uint32(time.Now().Unix()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("transfer", "error", err)
			return
		}
		if len(accs) == 0 {
			logger.Sugar().Infow("transfer", "Done", "...")
			return
		}

		for _, acc := range accs {
			if err := tryTransferOne(ctx, acc); err != nil {
				logger.Sugar().Errorw("transfer", "error", err)
			}
		}

		offset += limit
	}
}

func tryFinishOne(ctx context.Context, acc *depositmwpb.Account) error {
	tx, err := txmwcli.GetTx(ctx, acc.CollectingTID)
	if err != nil {
		return err
	}
	if tx == nil {
		return nil
	}

	coin, err := coinmwcli.GetCoin(ctx, tx.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return nil
	}

	outcoming := decimal.NewFromInt(0)

	switch tx.State {
	case basetypes.TxState_TxStateSuccessful:
		outcoming = outcoming.Add(decimal.RequireFromString(tx.Amount))
	case basetypes.TxState_TxStateFail:
	default:
		return nil
	}

	if err := accountlock.Lock(acc.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(acc.AccountID) //nolint
	}()

	scannableAt := uint32(time.Now().Unix() + timedef.SecondsPerHour)

	locked := false
	lockedBy := basetypes.AccountLockedBy_DefaultLockedBy
	collectingID := uuid1.InvalidUUIDStr
	outcomingS := outcoming.String()

	req := &depositmwpb.AccountReq{
		ID:            &acc.ID,
		AppID:         &acc.AppID,
		UserID:        &acc.UserID,
		CoinTypeID:    &acc.CoinTypeID,
		AccountID:     &acc.AccountID,
		Locked:        &locked,
		LockedBy:      &lockedBy,
		CollectingTID: &collectingID,
		ScannableAt:   &scannableAt,
	}
	if outcoming.Cmp(decimal.NewFromInt(0)) > 0 {
		req.Outcoming = &outcomingS
	}

	_, err = depositmwcli.UpdateAccount(ctx, req)

	return err
}

func finish(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	logger.Sugar().Infow("finish", "Start", "...")

	for {
		accs, _, err := depositmwcli.GetAccounts(ctx, &depositmwpb.Conds{
			Locked: &basetypes.BoolVal{
				Op:    cruder.EQ,
				Value: true,
			},
			LockedBy: &basetypes.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(basetypes.AccountLockedBy_Collecting),
			},
			ScannableAt: &basetypes.Uint32Val{
				Op:    cruder.LT,
				Value: uint32(time.Now().Unix()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("deposit", "error", err)
			return
		}
		if len(accs) == 0 {
			logger.Sugar().Infow("finish", "Done", "...")
			return
		}
		for _, acc := range accs {
			if err := tryFinishOne(ctx, acc); err != nil {
				logger.Sugar().Errorw("finish", "error", err)
			}
		}

		offset += limit
	}
}

var w *watcher.Watcher

func Watch(ctx context.Context) {
	depositTicker := time.NewTicker(60 * time.Second)
	transferTicker := time.NewTicker(4 * time.Hour)
	w = watcher.NewWatcher()

	for {
		select {
		case <-depositTicker.C:
			deposit(ctx)
			finish(ctx)
		case <-transferTicker.C:
			transfer(ctx)
		case <-ctx.Done():
			logger.Sugar().Infow(
				"Watch",
				"State", "Done",
				"Error", ctx.Err(),
			)
			close(w.ClosedChan())
			return
		case <-w.CloseChan():
			close(w.ClosedChan())
			return
		}
	}
}

func Shutdown() {
	if w != nil {
		w.Shutdown()
	}
}
