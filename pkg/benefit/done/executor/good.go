package executor

import (
	"context"
	"fmt"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/done/types"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent            chan interface{}
	notif                 chan interface{}
	done                  chan interface{}
	nextStartRewardAmount decimal.Decimal
	coin                  *coinmwpb.Coin
}

func (h *goodHandler) checkLeastTransferAmount() error {
	least, err := decimal.NewFromString(h.coin.LeastTransferAmount)
	if err != nil {
		return err
	}
	if least.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid leasttransferamount")
	}
	lastRewardAmount, err := decimal.NewFromString(h.LastRewardAmount)
	if err != nil {
		return err
	}
	if lastRewardAmount.Cmp(least) <= 0 {
		return nil
	}
	h.nextStartRewardAmount = h.nextStartRewardAmount.Sub(lastRewardAmount)
	return nil
}

func (h *goodHandler) getCoin(ctx context.Context) error {
	coin, err := coinmwcli.GetCoin(ctx, h.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.coin = coin
	return nil
}

//nolint:gocritic
func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Good", h.Good,
			"NextStartRewardAmount", h.nextStartRewardAmount,
			"Error", *err,
		)
	}

	persistentGood := &types.PersistentGood{
		Good:                  h.Good,
		NextStartRewardAmount: h.nextStartRewardAmount.String(),
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint
func (h *goodHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getCoin(ctx); err != nil {
		return err
	}
	h.nextStartRewardAmount, err = decimal.NewFromString(h.NextRewardStartAmount)
	if err != nil {
		return err
	}
	if err = h.checkLeastTransferAmount(); err != nil {
		return err
	}

	return nil
}
