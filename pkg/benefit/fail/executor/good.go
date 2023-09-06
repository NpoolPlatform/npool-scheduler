package executor

import (
	"context"

	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/fail/types"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent chan interface{}
}

func (h *goodHandler) final(ctx context.Context) {
	persistentGood := &types.PersistentGood{
		Good: h.Good,
	}
	asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
}

func (h *goodHandler) exec(ctx context.Context) error {
	h.final(ctx)
	return nil
}
