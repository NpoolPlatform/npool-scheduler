package executor

import (
	"context"
	"fmt"
	"time"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/transfer/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type accountHandler struct {
	*payaccmwpb.Account
	persistent     chan interface{}
	notif          chan interface{}
	done           chan interface{}
	amount         decimal.Decimal
	coin           *coinmwpb.Coin
	collectAccount *pltfaccmwpb.Account
}

func (h *accountHandler) getCoin(ctx context.Context, coinTypeID string) (*coinmwpb.Coin, error) {
	coin, err := coinmwcli.GetCoin(ctx, coinTypeID)
	if err != nil {
		return nil, err
	}
	if coin == nil {
		return nil, fmt.Errorf("invalid coin")
	}
	return coin, nil
}

func (h *accountHandler) checkAccountCoin() error {
	if h.collectAccount.CoinTypeID != h.CoinTypeID {
		return fmt.Errorf("invalid collect account coin")
	}
	return nil
}

func (h *accountHandler) recheckAccount(ctx context.Context) (bool, error) {
	account, err := payaccmwcli.GetAccount(ctx, h.ID)
	if err != nil {
		return false, err
	}
	if account == nil {
		return false, fmt.Errorf("invalid account")
	}
	if account.Locked || account.Blocked || !account.Active {
		return false, nil
	}
	if account.AvailableAt >= uint32(time.Now().Unix()) {
		return false, nil
	}
	return true, nil
}

func (h *accountHandler) checkBalance(ctx context.Context) error {
	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.coin.Name,
		Address: h.Address,
	})
	if err != nil {
		return err
	}
	if bal == nil {
		return fmt.Errorf("invalid balance")
	}

	balance, err := decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}

	limit, err := decimal.NewFromString(h.coin.PaymentAccountCollectAmount)
	if err != nil {
		return err
	}
	reserved, err := decimal.NewFromString(h.coin.ReservedAmount)
	if err != nil {
		return err
	}
	if balance.Cmp(limit) < 0 || balance.Cmp(reserved) <= 0 {
		return nil
	}
	h.amount = balance.Sub(reserved)
	return nil
}

func (h *accountHandler) checkFeeBalance(ctx context.Context) error {
	if h.coin.ID == h.coin.FeeCoinTypeID {
		return nil
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.coin.FeeCoinName,
		Address: h.Address,
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
	if bal.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("insufficient gas")
	}

	return nil
}

func (h *accountHandler) getCollectAccount(ctx context.Context) error {
	account, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.coin.ID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountUsedFor_PaymentCollector)},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid collect account")
	}
	h.collectAccount = account
	return nil
}

func (h *accountHandler) checkTransferring(ctx context.Context) (bool, error) {
	exist, err := txmwcli.ExistTxConds(ctx, &txmwpb.Conds{
		AccountID: &basetypes.StringVal{Op: cruder.EQ, Value: h.AccountID},
		States: &basetypes.Uint32SliceVal{Op: cruder.IN, Value: []uint32{
			uint32(basetypes.TxState_TxStateCreated),
			uint32(basetypes.TxState_TxStateCreatedCheck),
			uint32(basetypes.TxState_TxStateWait),
			uint32(basetypes.TxState_TxStateWaitCheck),
			uint32(basetypes.TxState_TxStateTransferring),
		}},
		Type: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxType_TxPaymentCollect)},
	})
	if err != nil {
		return false, err
	}
	if exist {
		return true, nil
	}

	txs, _, err := txmwcli.GetTxs(ctx, &txmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		AccountID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.AccountID},
		State:      &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxState_TxStateSuccessful)},
		Type:       &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxType_TxPaymentCollect)},
	}, int32(0), int32(1))
	if err != nil {
		return false, err
	}
	if len(txs) == 0 {
		return false, nil
	}
	const coolDown = timedef.SecondsPerHour
	if txs[0].CreatedAt+coolDown > uint32(time.Now().Unix()) {
		return true, nil
	}
	return false, nil
}

//nolint:gocritic
func (h *accountHandler) final(ctx context.Context, err *error) {
	if *err != nil || true {
		logger.Sugar().Errorw(
			"final",
			"Account", h,
			"Coin", h.coin,
			"Amount", h.amount,
			"CollectAccount", h.collectAccount,
			"Error", *err,
		)
	}

	persistentAccount := &types.PersistentAccount{
		Account:          h.Account,
		CollectAmount:    h.amount.String(),
		FeeAmount:        decimal.NewFromInt(0).String(),
		PaymentAccountID: h.AccountID,
		PaymentAddress:   h.Address,
		Error:            *err,
	}
	if h.collectAccount != nil {
		persistentAccount.CollectAccountID = h.collectAccount.AccountID
		persistentAccount.CollectAddress = h.collectAccount.Address
	}

	if h.amount.Cmp(decimal.NewFromInt(0)) <= 0 && *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentAccount, h.done)
		return
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentAccount, h.persistent)
	} else {
		asyncfeed.AsyncFeed(ctx, persistentAccount, h.notif)
		asyncfeed.AsyncFeed(ctx, persistentAccount, h.done)
	}
}

//nolint:gocritic
func (h *accountHandler) exec(ctx context.Context) error {
	if h.Locked {
		return nil
	}

	var err error
	var executable bool
	var yes bool

	defer h.final(ctx, &err)

	h.coin, err = h.getCoin(ctx, h.CoinTypeID)
	if err != nil {
		return err
	}
	if err = h.getCollectAccount(ctx); err != nil {
		return err
	}
	if err = h.checkAccountCoin(); err != nil {
		return err
	}
	if err = accountlock.Lock(h.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(h.AccountID) //nolint
	}()
	if executable, err = h.recheckAccount(ctx); err != nil || !executable {
		return err
	}
	if yes, err = h.checkTransferring(ctx); err != nil || yes {
		return err
	}
	if err = h.checkFeeBalance(ctx); err != nil {
		return err
	}
	if err = h.checkBalance(ctx); err != nil {
		return err
	}
	return nil
}
