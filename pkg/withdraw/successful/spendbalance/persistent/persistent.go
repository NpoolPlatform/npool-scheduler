package persistent

import (
	"context"
	"fmt"

	withdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/spendbalance/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, withdraw interface{}, reward, notif, done chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	defer asyncfeed.AsyncFeed(ctx, _withdraw, done)

	state := ledgertypes.WithdrawState_Successful
	if _, err := withdrawmwcli.UpdateWithdraw(ctx, &withdrawmwpb.WithdrawReq{
		ID:        &_withdraw.ID,
		State:     &state,
		FeeAmount: &_withdraw.WithdrawFeeAmount,
	}); err != nil {
		return err
	}

	asyncfeed.AsyncFeed(ctx, _withdraw, notif)

	return nil
}
