package executor

import (
	"context"

	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/done/types"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent chan interface{}
}

func (h *goodHandler) final() {
	persistentGood := &types.PersistentGood{
		Good: h.Good,
	}
	h.persistent <- persistentGood
}

func (h *goodHandler) exec(ctx context.Context) error {
	h.final()
	return nil
}
