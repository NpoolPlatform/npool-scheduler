package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/rejected/returnbalance/types"

	"github.com/shopspring/decimal"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent          chan interface{}
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
		cancelablefeed.CancelableFeed(ctx, persistentWithdraw, h.persistent)
	}
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
