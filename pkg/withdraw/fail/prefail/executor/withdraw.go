package executor

import (
	"context"

	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/fail/prefail/types"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent chan interface{}
}

func (h *withdrawHandler) final(ctx context.Context) {
	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw: h.Withdraw,
	}
	asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.persistent)
}

func (h *withdrawHandler) exec(ctx context.Context) error {
	h.final(ctx)
	return nil
}
