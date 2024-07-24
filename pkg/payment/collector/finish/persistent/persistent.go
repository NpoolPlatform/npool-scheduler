package persistent

import (
	"context"
	"fmt"

	paymentaccountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/collector/finish/types"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, account interface{}, reward, notif, done chan interface{}) error {
	_account, ok := account.(*types.PersistentAccount)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	defer asyncfeed.AsyncFeed(ctx, _account, done)

	if err := accountlock.Lock(_account.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(_account.AccountID) //nolint
	}()

	locked := false
	collectingID := uuid.Nil.String()
	if _, err := paymentaccountmwcli.UpdateAccount(ctx, &paymentaccountmwpb.AccountReq{
		ID:            &_account.ID,
		CoinTypeID:    &_account.CoinTypeID,
		AccountID:     &_account.AccountID,
		Locked:        &locked,
		CollectingTID: &collectingID,
	}); err != nil {
		return err
	}

	return nil
}
