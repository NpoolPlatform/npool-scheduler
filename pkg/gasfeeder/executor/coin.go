package executor

import (
	"context"

	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
)

type coinHandler struct {
	*coinmwpb.Coin
	persistent chan *types.PersistentCoin
	notif      chan *types.PersistentCoin
}

func (h *coinHandler) exec(ctx context.Context) error {
	return nil
}
