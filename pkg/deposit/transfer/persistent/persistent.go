package persistent

import (
	"context"
	"fmt"

	depositaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/transfer/types"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

// Here we could not use dtm to create transfer
func (p *handler) Update(ctx context.Context, account interface{}, retry, notif, done chan interface{}) error {
	_account, ok := account.(*types.PersistentAccount)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	if _account.CollectingTIDCandidate == nil {
		collectingTID := uuid.NewString()
		_account.CollectingTIDCandidate = &collectingTID
	}

	if !_account.Locked {
		if err := accountlock.Lock(_account.DepositAccountID); err != nil {
			return err
		}
		defer func() {
			_ = accountlock.Unlock(_account.DepositAccountID)
		}()

		locked := true
		lockedBy := basetypes.AccountLockedBy_Collecting

		if _, err := depositaccmwcli.UpdateAccount(ctx, &depositaccmwpb.AccountReq{
			ID:            &_account.ID,
			AppID:         &_account.AppID,
			UserID:        &_account.UserID,
			CoinTypeID:    &_account.CoinTypeID,
			AccountID:     &_account.DepositAccountID,
			Locked:        &locked,
			LockedBy:      &lockedBy,
			CollectingTID: _account.CollectingTIDCandidate,
		}); err != nil {
			retry1.Retry(ctx, _account, retry)
			return err
		}
		_account.Locked = true
	}

	txType := basetypes.TxType_TxPaymentCollect
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		ID:            _account.CollectingTIDCandidate,
		CoinTypeID:    &_account.CoinTypeID,
		FromAccountID: &_account.DepositAccountID,
		ToAccountID:   &_account.CollectAccountID,
		Amount:        &_account.CollectAmount,
		FeeAmount:     &_account.FeeAmount,
		Type:          &txType,
	}); err != nil {
		return err
	}

	asyncfeed.AsyncFeed(_account, done)

	return nil
}
