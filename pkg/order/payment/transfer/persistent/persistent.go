package persistent

import (
	"context"
	"fmt"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/transfer/types"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

// Here we could not use dtm to create transfer
func (p *handler) Update(ctx context.Context, account interface{}, notif, done chan interface{}) error {
	_account, ok := account.(*types.PersistentAccount)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	defer asyncfeed.AsyncFeed(ctx, _account, done)

	if _account.CollectingTIDCandidate == nil {
		collectingTID := uuid.NewString()
		_account.CollectingTIDCandidate = &collectingTID
	}

	if !_account.Locked {
		if err := accountlock.Lock(_account.PaymentAccountID); err != nil {
			return err
		}
		defer func() {
			_ = accountlock.Unlock(_account.PaymentAccountID) //nolint
		}()

		locked := true
		lockedBy := basetypes.AccountLockedBy_Collecting

		if _, err := payaccmwcli.UpdateAccount(ctx, &payaccmwpb.AccountReq{
			ID:            &_account.ID,
			CoinTypeID:    &_account.CoinTypeID,
			AccountID:     &_account.PaymentAccountID,
			Locked:        &locked,
			LockedBy:      &lockedBy,
			CollectingTID: _account.CollectingTIDCandidate,
		}); err != nil {
			return err
		}
		_account.Locked = true
	}

	txType := basetypes.TxType_TxPaymentCollect
	if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
		ID:            _account.CollectingTIDCandidate,
		CoinTypeID:    &_account.CoinTypeID,
		FromAccountID: &_account.PaymentAccountID,
		ToAccountID:   &_account.CollectAccountID,
		Amount:        &_account.CollectAmount,
		FeeAmount:     &_account.FeeAmount,
		Type:          &txType,
	}); err != nil {
		return err
	}

	return nil
}
