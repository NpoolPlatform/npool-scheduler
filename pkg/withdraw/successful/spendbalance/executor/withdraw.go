package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/fail/returnbalance/types"

	"github.com/shopspring/decimal"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent          chan interface{}
	notif               chan interface{}
	lockedBalanceAmount decimal.Decimal
}

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
		Withdraw: h.Withdraw,
	}
	if h.lockedBalanceAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := h.lockedBalanceAmount.String()
		persistentWithdraw.LockedBalanceAmount = &amount
	}
	if h.spentBalanceAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		amount := h.spentBalanceAmount.String()
		persistentWithdraw.SpentAmount = &amount
		ioExtra := fmt.Sprintf(
			`{"AppID":"%v","UserID":"%v","WithdrawID":"%v","Amount":"%v","Date":"%v","CancelWithdraw":true}`,
			h.AppID,
			h.UserID,
			h.ID,
			h.spentBalanceAmount,
			time.Now(),
		)
		persistentWithdraw.SpentExtra = ioExtra
	}

	if *err == nil {
		asyncfeed.AsyncFeed(persistentWithdraw, h.persistent)
	} else {
		asyncfeed.AsyncFeed(persistentWithdraw, h.notif)
	}
}

func (h *withdrawHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	h.lockedBalanceAmount, err = decimal.NewFromString(h.Amount)
	if err != nil {
		return err
	}

	return nil
}
