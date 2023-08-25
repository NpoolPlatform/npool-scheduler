package sentinel

import (
	"context"
	"time"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"

	"github.com/google/uuid"
)

type handler struct {
	basesentinel.Sentinel
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	h = &handler{
		Sentinel: basesentinel.NewSentinel(ctx, cancel, h, time.Minute),
	}
}

func (h *handler) Scan(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		coins, _, err := coinmwcli.GetCoins(ctx, &coinmwpb.Conds{}, offset, limit)
		if err != nil {
			return err
		}
		if len(coins) == 0 {
			return nil
		}

		for _, coin := range coins {
			if _, err := uuid.Parse(coin.FeeCoinTypeID); err != nil {
				continue
			}
			if coin.FeeCoinTypeID == uuid.Nil.String() {
				continue
			}
			if coin.FeeCoinTypeID == coin.ID {
				continue
			}
			h.Exec() <- coin
		}

		offset += limit
	}
}

func Exec() chan interface{} {
	return h.Exec()
}

func Finalize() {
	if h != nil {
		h.Finalize()
	}
}
