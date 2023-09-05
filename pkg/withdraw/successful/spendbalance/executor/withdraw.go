package executor

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/spendbalance/types"

	"github.com/shopspring/decimal"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent          chan interface{}
	lockedBalanceAmount decimal.Decimal
	withdrawFeeAmount   decimal.Decimal
}

func (h *withdrawHandler) getWithdrawFeeAmount(ctx context.Context) error {
	tx, err := txmwcli.GetTx(ctx, h.PlatformTransactionID)
	if err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("invalid tx")
	}
	amount, err := decimal.NewFromString(tx.FeeAmount)
	if err != nil {
		return err
	}
	h.withdrawFeeAmount = amount
	return nil
}

//nolint:gocritic
func (h *withdrawHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Withdraw", h.Withdraw,
			"LockedBalance", h.lockedBalanceAmount,
			"WithdrawFeeAmount", h.withdrawFeeAmount,
			"Error", *err,
		)
	}
	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw:            h.Withdraw,
		LockedBalanceAmount: h.lockedBalanceAmount.String(),
		WithdrawFeeAmount:   h.withdrawFeeAmount.String(),
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.persistent)
	}
}

func (h *withdrawHandler) exec(ctx context.Context) error { //nolint
	var err error

	defer h.final(ctx, &err)

	if err = h.getWithdrawFeeAmount(ctx); err != nil {
		return err
	}
	h.lockedBalanceAmount, err = decimal.NewFromString(h.Amount)
	if err != nil {
		return err
	}

	return nil
}
