package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/rejected/returnbalance/types"

	"github.com/shopspring/decimal"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent          chan interface{}
	done                chan interface{}
	lockedBalanceAmount decimal.Decimal
}

//nolint:gocritic
func (h *withdrawHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Withdraw", h.Withdraw,
			"LockedBalance", h.lockedBalanceAmount,
			"Error", *err,
		)
	}
	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw:            h.Withdraw,
		LockedBalanceAmount: h.lockedBalanceAmount.String(),
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.done)
}

//nolint
func (h *withdrawHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	h.lockedBalanceAmount, err = decimal.NewFromString(h.Amount)
	if err != nil {
		return err
	}

	return nil
}
