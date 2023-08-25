package executor

import (
	"context"
	"fmt"
	"time"

	accountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/account"
	useraccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/user"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	accountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/account"
	useraccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/user"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/wait/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type txHandler struct {
	*txmwpb.Tx
	retry            chan interface{}
	persistent       chan interface{}
	newState         basetypes.TxState
	transactionExist bool
	fromAccount      *accountmwpb.Account
	toAccount        *accountmwpb.Account
	transferAmount   decimal.Decimal
	coin             *coinmwpb.Coin
	memo             *string
}

func (h *txHandler) checkTransfer(ctx context.Context) (bool, error) {
	tx, err := sphinxproxycli.GetTransaction(ctx, h.ID)
	if err != nil {
		return false, err
	}
	if tx == nil {
		return false, nil
	}
	h.newState = basetypes.TxState_TxStateTransferring
	h.transactionExist = true
	return true, nil
}

func (h *txHandler) getAccount(ctx context.Context, accountID string) (*accountmwpb.Account, error) {
	account, err := accountmwcli.GetAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("invlaid")
	}
	return account, nil
}

func (h *txHandler) getCoin(ctx context.Context) error {
	coin, err := coinmwcli.GetCoin(ctx, h.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.coin = coin
	return nil
}

func (h *txHandler) checkTransferAmount(ctx context.Context) error {
	bal, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.coin.Name,
		Address: h.fromAccount.Address,
	})
	if err != nil {
		return err
	}
	if bal == nil {
		return fmt.Errorf("invalid balance")
	}

	amount, err := decimal.NewFromString(h.Amount)
	if err != nil {
		return err
	}
	feeAmount, err := decimal.NewFromString(h.FeeAmount)
	if err != nil {
		return err
	}
	balance, err := decimal.NewFromString(bal.BalanceStr)
	if err != nil {
		return err
	}
	reserved, err := decimal.NewFromString(h.coin.ReservedAmount)
	if err != nil {
		return err
	}
	if amount.Cmp(feeAmount) <= 0 {
		return fmt.Errorf("invalid amount")
	}

	h.transferAmount = amount.Sub(feeAmount)

	if h.transferAmount.Add(reserved).Cmp(balance) < 0 {
		return fmt.Errorf("insufficient funds")
	}

	return nil
}

func (h *txHandler) getMemo(ctx context.Context) error {
	if h.Type != basetypes.TxType_TxWithdraw {
		return nil
	}

	account, err := useraccmwcli.GetAccountOnly(ctx, &useraccmwpb.Conds{
		AccountID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.ToAccountID},
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.UsedFor_Withdraw)},
	})
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid useraccount")
	}
	if account.Memo == "" {
		return nil
	}
	h.memo = &account.Memo
	return nil
}

func (h *txHandler) final(ctx context.Context) {
	if h.newState == h.State {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				h.retry <- h.Tx
			}
		}()
		return
	}

	persistentTx := &types.PersistentTx{
		Tx:               h.Tx,
		TransactionExist: h.transactionExist,
		CoinName:         h.coin.Name,
		FromAddress:      h.fromAccount.Address,
		ToAddress:        h.toAccount.Address,
		Amount:           h.transferAmount.String(),
		FloatAmount:      h.transferAmount.InexactFloat64(),
		AccountMemo:      h.memo,
	}
	h.persistent <- persistentTx
}

func (h *txHandler) exec(ctx context.Context) error {
	defer h.final(ctx)

	if exist, err := h.checkTransfer(ctx); err != nil || exist {
		return err
	}
	var err error
	h.fromAccount, err = h.getAccount(ctx, h.FromAccountID)
	if err != nil {
		return err
	}
	h.toAccount, err = h.getAccount(ctx, h.ToAccountID)
	if err != nil {
		return err
	}
	if err := h.getCoin(ctx); err != nil {
		return err
	}
	if err := h.checkTransferAmount(ctx); err != nil {
		return err
	}
	if err := h.getMemo(ctx); err != nil {
		return err
	}

	h.newState = basetypes.TxState_TxStateTransferring

	return nil
}
