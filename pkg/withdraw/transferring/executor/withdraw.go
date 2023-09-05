package executor

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/transferring/types"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent       chan interface{}
	notif            chan interface{}
	newWithdrawState ledgertypes.WithdrawState
	chainTxID        string
}

func (h *withdrawHandler) checkTransfer(ctx context.Context) error {
	tx, err := txmwcli.GetTx(ctx, h.PlatformTransactionID)
	if err != nil {
		return err
	}
	if tx == nil {
		h.newWithdrawState = ledgertypes.WithdrawState_PreFail
		return fmt.Errorf("invalid tx")
	}
	switch tx.State {
	case basetypes.TxState_TxStateSuccessful:
		h.newWithdrawState = ledgertypes.WithdrawState_PreSuccessful
	case basetypes.TxState_TxStateFail:
		h.newWithdrawState = ledgertypes.WithdrawState_PreFail
	}
	h.chainTxID = tx.ChainTxID
	return nil
}

//nolint:gocritic
func (h *withdrawHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Withdraw", h.Withdraw,
			"NewWithdrawState", h.newWithdrawState,
			"ChainTxID", h.chainTxID,
			"Error", *err,
		)
	}
	if h.newWithdrawState == h.State && *err == nil {
		return
	}
	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw:         h.Withdraw,
		NewWithdrawState: h.newWithdrawState,
		ChainTxID:        h.chainTxID,
		Error:            *err,
	}
	if h.newWithdrawState != h.State {
		asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.persistent)
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, persistentWithdraw, h.notif)
	}
}

//nolint:gocritic
func (h *withdrawHandler) exec(ctx context.Context) error {
	h.newWithdrawState = h.State

	var err error
	defer h.final(ctx, &err)

	if err = h.checkTransfer(ctx); err != nil {
		return err
	}

	return nil
}
