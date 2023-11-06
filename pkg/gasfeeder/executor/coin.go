//nolint:dupl
package executor

import (
	"context"
	"fmt"
	"time"

	accountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/account"
	depositaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	accountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/account"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type coinHandler struct {
	*coinmwpb.Coin
	persistent         chan interface{}
	notif              chan interface{}
	done               chan interface{}
	gasProviderAccount *accountmwpb.Account
}

func (h *coinHandler) getPlatformAccount(ctx context.Context, coinTypeID string, usedFor basetypes.AccountUsedFor) (*pltfaccmwpb.Account, error) {
	account, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: coinTypeID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(usedFor)},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return nil, err
	}
	if account == nil || account.Address == "" {
		return nil, fmt.Errorf("invalid ")
	}
	return account, nil
}

func (h *coinHandler) getGasProvider(ctx context.Context) error {
	account, err := h.getPlatformAccount(ctx, h.FeeCoinTypeID, basetypes.AccountUsedFor_GasProvider)
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid gasprovider")
	}

	_account, err := accountmwcli.GetAccount(ctx, account.AccountID)
	if err != nil {
		return err
	}
	if _account == nil {
		return fmt.Errorf("invalid gasprovider")
	}

	h.gasProviderAccount = _account
	return nil
}

func (h *coinHandler) feeding(ctx context.Context, account *accountmwpb.Account) (bool, error) {
	txs, _, err := txmwcli.GetTxs(ctx, &txmwpb.Conds{
		AccountID: &basetypes.StringVal{Op: cruder.EQ, Value: account.EntID},
		Type:      &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.TxType_TxFeedGas)},
		States: &basetypes.Uint32SliceVal{Op: cruder.IN, Value: []uint32{
			uint32(basetypes.TxState_TxStateCreated),
			uint32(basetypes.TxState_TxStateCreatedCheck),
			uint32(basetypes.TxState_TxStateWait),
			uint32(basetypes.TxState_TxStateWaitCheck),
			uint32(basetypes.TxState_TxStateTransferring),
			uint32(basetypes.TxState_TxStateSuccessful),
		}},
	}, int32(0), int32(1))
	if err != nil {
		return false, err
	}
	if len(txs) == 0 {
		return false, nil
	}

	if txs[0].State != basetypes.TxState_TxStateSuccessful {
		logger.Sugar().Debugw(
			"feeding",
			"Account", account,
			"Txs", txs,
			"State", "Feeding",
		)
		return true, nil
	}

	const coolDown = uint32(10 * timedef.SecondsPerMinute)
	if txs[0].UpdatedAt+coolDown > uint32(time.Now().Unix()) {
		logger.Sugar().Debugw(
			"feeding",
			"Account", account,
			"Txs", txs,
			"State", "Feeding",
		)
		return true, nil
	}

	return false, nil
}

func (h *coinHandler) enough(ctx context.Context, account *accountmwpb.Account, amount decimal.Decimal) (bool, error) {
	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.FeeCoinName,
		Address: account.Address,
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
	logger.Sugar().Debugw(
		"enough",
		"Account", account,
		"Amount", amount,
		"Balance", bal,
	)
	return bal.Cmp(amount) >= 0, nil
}

func (h *coinHandler) feedable(ctx context.Context, account *accountmwpb.Account, amount, low decimal.Decimal) (bool, error) {
	if enough, err := h.enough(ctx, account, low); err != nil || enough {
		logger.Sugar().Debugw(
			"feedable",
			"Account", account,
			"Amount", amount,
			"Low", low,
			"Enough", enough,
			"Error", err,
		)
		return false, err
	}
	if feeding, err := h.feeding(ctx, account); err != nil || feeding {
		logger.Sugar().Debugw(
			"feedable",
			"Account", account,
			"Amount", amount,
			"Low", low,
			"Feeding", feeding,
			"Error", err,
		)
		return false, err
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.Name,
		Address: account.Address,
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
	reserved, err := decimal.NewFromString(h.ReservedAmount)
	if err != nil {
		return false, err
	}
	if bal.Cmp(reserved) <= 0 {
		logger.Sugar().Debugw(
			"feedable",
			"Account", account,
			"Amount", amount,
			"Low", low,
			"Reserved", reserved,
			"Balance", bal,
			"Error", err,
		)
		return false, nil
	}

	enough, err := h.enough(ctx, h.gasProviderAccount, amount)
	if err != nil {
		return false, err
	}
	if !enough {
		return false, fmt.Errorf("insufficient funds")
	}
	return true, nil
}

func (h *coinHandler) checkUserBenefitHot(ctx context.Context) (bool, *accountmwpb.Account, decimal.Decimal, error) {
	account, err := h.getPlatformAccount(ctx, h.EntID, basetypes.AccountUsedFor_UserBenefitHot)
	if err != nil {
		return false, nil, decimal.NewFromInt(0), err
	}
	if account == nil {
		return false, nil, decimal.NewFromInt(0), fmt.Errorf("invalid account")
	}

	_account, err := accountmwcli.GetAccount(ctx, account.AccountID)
	if err != nil {
		return false, nil, decimal.NewFromInt(0), err
	}
	if _account == nil {
		return false, nil, decimal.NewFromInt(0), fmt.Errorf("invalid account")
	}

	amount, err := decimal.NewFromString(h.HotWalletFeeAmount)
	if err != nil {
		return false, _account, decimal.NewFromInt(0), err
	}
	lowFeeAmount, err := decimal.NewFromString(h.HotLowFeeAmount)
	if err != nil {
		return false, _account, decimal.NewFromInt(0), err
	}

	feedable, err := h.feedable(ctx, _account, amount, lowFeeAmount)
	if err != nil {
		return false, _account, decimal.NewFromInt(0), err
	}
	return feedable, _account, amount, nil
}

func (h *coinHandler) checkPaymentAccount(ctx context.Context) (bool, *accountmwpb.Account, decimal.Decimal, error) {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	amount, err := decimal.NewFromString(h.CollectFeeAmount)
	if err != nil {
		return false, nil, decimal.NewFromInt(0), err
	}
	lowFeeAmount, err := decimal.NewFromString(h.LowFeeAmount)
	if err != nil {
		return false, nil, decimal.NewFromInt(0), err
	}

	for {
		accounts, _, err := payaccmwcli.GetAccounts(ctx, &payaccmwpb.Conds{
			CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
			Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
			Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		}, offset, limit)
		if err != nil {
			return false, nil, decimal.NewFromInt(0), err
		}
		if len(accounts) == 0 {
			return false, nil, decimal.NewFromInt(0), nil
		}

		ids := []string{}
		for _, account := range accounts {
			ids = append(ids, account.AccountID)
		}
		_accounts, _, err := accountmwcli.GetAccounts(ctx, &accountmwpb.Conds{
			EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: ids},
		}, int32(0), int32(len(ids)))
		if err != nil {
			return false, nil, decimal.NewFromInt(0), err
		}

		for _, account := range _accounts {
			if feedable, err := h.feedable(ctx, account, amount, lowFeeAmount); err != nil || feedable {
				return feedable, account, amount, err
			}
		}

		offset += limit
	}
}

func (h *coinHandler) checkDepositAccount(ctx context.Context) (bool, *accountmwpb.Account, decimal.Decimal, error) {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	amount, err := decimal.NewFromString(h.CollectFeeAmount)
	if err != nil {
		return false, nil, decimal.NewFromInt(0), err
	}
	lowFeeAmount, err := decimal.NewFromString(h.LowFeeAmount)
	if err != nil {
		return false, nil, decimal.NewFromInt(0), err
	}

	for {
		accounts, _, err := depositaccmwcli.GetAccounts(ctx, &depositaccmwpb.Conds{
			CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
			Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
			Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		}, offset, limit)
		if err != nil {
			return false, nil, decimal.NewFromInt(0), err
		}
		if len(accounts) == 0 {
			return false, nil, decimal.NewFromInt(0), nil
		}

		ids := []string{}
		for _, account := range accounts {
			ids = append(ids, account.AccountID)
		}
		_accounts, _, err := accountmwcli.GetAccounts(ctx, &accountmwpb.Conds{
			EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: ids},
		}, int32(0), int32(len(ids)))
		if err != nil {
			return false, nil, decimal.NewFromInt(0), err
		}

		for _, account := range _accounts {
			if feedable, err := h.feedable(ctx, account, amount, lowFeeAmount); err != nil || feedable {
				return feedable, account, amount, err
			}
		}

		offset += limit
	}
}

// TODO: in case some mining product get rewards other than it native coin (native coin is fee coin)
func (h *coinHandler) checkGoodBenefit(ctx context.Context) (bool, *accountmwpb.Account, decimal.Decimal, error) { //nolint
	return false, nil, decimal.NewFromInt(0), nil
}

//nolint:gocritic,interfacer
func (h *coinHandler) final(ctx context.Context, account **accountmwpb.Account, usedFor *basetypes.AccountUsedFor, amount *decimal.Decimal, feedable *bool, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Coin", h.Coin,
			"GasProvider", h.gasProviderAccount,
			"Amount", amount,
			"UsedFor", *usedFor,
			"Account", *account,
			"Feedable", *feedable,
			"Error", *err,
		)
	}

	persistentCoin := &types.PersistentCoin{
		Coin:      h.Coin,
		Amount:    amount.String(),
		FeeAmount: decimal.NewFromInt(0).String(),
		UsedFor:   *usedFor,
		Extra:     fmt.Sprintf(`{"Coin":"%v","FeeCoin":"%v","Type":"%v"}`, h.Name, h.FeeCoinName, *usedFor),
		Error:     *err,
	}
	if *account != nil {
		persistentCoin.ToAccountID = (*account).EntID
		persistentCoin.ToAddress = (*account).Address
	}
	if h.gasProviderAccount != nil {
		persistentCoin.FromAccountID = h.gasProviderAccount.EntID
		persistentCoin.FromAddress = h.gasProviderAccount.Address
	}

	if !*feedable && *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentCoin, h.done)
		return
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, persistentCoin, h.notif)
	}
	if *feedable {
		asyncfeed.AsyncFeed(ctx, persistentCoin, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentCoin, h.done)
}

//nolint:gocritic
func (h *coinHandler) exec(ctx context.Context) error {
	var err error
	var account *accountmwpb.Account
	var amount decimal.Decimal
	var feedable bool
	var usedFor basetypes.AccountUsedFor
	var feeding bool

	defer h.final(ctx, &account, &usedFor, &amount, &feedable, &err)

	if err = h.getGasProvider(ctx); err != nil {
		return err
	}
	if feeding, err = h.feeding(ctx, h.gasProviderAccount); err != nil || feeding {
		return err
	}
	if feedable, account, amount, err = h.checkUserBenefitHot(ctx); err != nil || feedable {
		usedFor = basetypes.AccountUsedFor_UserBenefitHot
		return err
	}
	if feedable, account, amount, err = h.checkPaymentAccount(ctx); err != nil || feedable {
		usedFor = basetypes.AccountUsedFor_GoodPayment
		return err
	}
	if feedable, account, amount, err = h.checkDepositAccount(ctx); err != nil || feedable {
		usedFor = basetypes.AccountUsedFor_UserDeposit
		return err
	}
	if feedable, account, amount, err = h.checkGoodBenefit(ctx); err != nil || feedable {
		usedFor = basetypes.AccountUsedFor_GoodBenefit
		return err
	}

	return nil
}
