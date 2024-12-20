package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	goodpledgemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/pledge"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/pledge/wait/types"
)

type pledgeHandler struct {
	*goodpledgemwpb.Pledge
	persistent chan interface{}
	notif      chan interface{}
	done       chan interface{}
}

//nolint:gocritic
func (h *pledgeHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Pledge Good", h.Pledge,
			"Error", *err,
		)
	}

	persistentPledge := &types.PersistentGoodPledge{
		Pledge: h.Pledge,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentPledge, h.persistent)
	} else {
		asyncfeed.AsyncFeed(ctx, persistentPledge, h.done)
	}
}

func (h *pledgeHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	return nil
}
