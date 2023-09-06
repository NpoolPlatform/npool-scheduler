package persistent

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	withdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/approved/types"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, withdraw interface{}, notif, done chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	defer asyncfeed.AsyncFeed(ctx, _withdraw, done)

	req := &withdrawmwpb.WithdrawReq{
		ID:    &_withdraw.ID,
		State: &_withdraw.NewWithdrawState,
	}
	if _withdraw.NewWithdrawState == ledgertypes.WithdrawState_Transferring {
		id := uuid.NewString()
		req.PlatformTransactionID = &id
	}
	if _, err := withdrawmwcli.UpdateWithdraw(ctx, req); err != nil {
		return err
	}
	if _withdraw.NewWithdrawState == ledgertypes.WithdrawState_Transferring {
		txType := basetypes.TxType_TxWithdraw
		if _, err := txmwcli.CreateTx(ctx, &txmwpb.TxReq{
			ID:            req.PlatformTransactionID,
			CoinTypeID:    &_withdraw.CoinTypeID,
			FromAccountID: &_withdraw.UserBenefitHotAccountID,
			ToAccountID:   &_withdraw.AccountID,
			Amount:        &_withdraw.WithdrawAmount,
			FeeAmount:     &_withdraw.WithdrawFeeAmount,
			Type:          &txType,
			Extra:         &_withdraw.WithdrawExtra,
		}); err != nil {
			return err
		}
	}

	return nil
}
