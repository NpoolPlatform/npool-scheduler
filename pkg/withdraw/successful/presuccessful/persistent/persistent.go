package persistent

import (
	"context"
	"fmt"

	withdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/presuccessful/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, withdraw interface{}, retry, notif, done chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	state := ledgertypes.WithdrawState_SpendSuccessfulBalance
	if _, err := withdrawmwcli.UpdateWithdraw(ctx, &withdrawmwpb.WithdrawReq{
		ID:    &_withdraw.ID,
		State: &state,
	}); err != nil {
		retry1.Retry(ctx, _withdraw, retry)
		return err
	}

	cancelablefeed.CancelableFeed(ctx, _withdraw, done)

	return nil
}
