package executor

import (
	"context"

	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/done/types"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent          chan interface{}
	notif               chan interface{}
	newUnitRewardAmount decimal.Decimal
}

//nolint:gocritic
func (h *goodHandler) final(err *error) {
	if *err == nil {
		return
	}

	persistentGood := &types.PersistentGood{
		Good:                h.Good,
		NewUnitRewardAmount: h.newUnitRewardAmount.String(),
	}

	if *err == nil {
		asyncfeed.AsyncFeed(persistentGood, h.persistent)
	} else {
		asyncfeed.AsyncFeed(persistentGood, h.notif)
	}
}

//nolint
func (h *goodHandler) exec(ctx context.Context) error {
	var err error
	var rewardAmount decimal.Decimal
	var totalUnits decimal.Decimal

	defer h.final(&err)

	rewardAmount, err = decimal.NewFromString(h.LastRewardAmount)
	if err != nil {
		return err
	}
	totalUnits, err = decimal.NewFromString(h.GoodTotal)
	if err != nil {
		return err
	}
	h.newUnitRewardAmount = rewardAmount.Mul(totalUnits)

	return nil
}
