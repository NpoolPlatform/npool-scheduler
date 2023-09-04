package executor

import (
	"context"

	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/presuccessful/types"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent chan interface{}
}

func (h *withdrawHandler) final(ctx context.Context) {
	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw: h.Withdraw,
	}
	cancelablefeed.CancelableFeed(ctx, persistentWithdraw, h.persistent)
}

func (h *withdrawHandler) exec(ctx context.Context) error { //nolint
	h.final(ctx)
	return nil
}
