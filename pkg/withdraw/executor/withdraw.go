package executor

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/types"
)

type withdrawHandler struct {
	*withdrawmwpb.Withdraw
	persistent chan interface{}
	notif      chan interface{}
	newState   ledgertypes.WithdrawState
	chainTxID  string
}

func (h *withdrawHandler) checkTransfer(ctx context.Context) error {
	tx, err := txmwcli.GetTx(ctx, h.PlatformTransactionID)
	if err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("invalid tx")
	}

	switch tx.State {
	case basetypes.TxState_TxStateFail:
		h.newState = ledgertypes.WithdrawState_TransactionFail
	case basetypes.TxState_TxStateSuccessful:
		h.newState = ledgertypes.WithdrawState_Successful
	default:
		return nil
	}

	h.chainTxID = tx.ChainTxID

	return nil
}

func (h *withdrawHandler) final(ctx context.Context, err *error) {
	if h.newState == h.State && *err == nil {
		return
	}

	persistentWithdraw := &types.PersistentWithdraw{
		Withdraw:  h.Withdraw,
		NewState:  h.newState,
		ChainTxID: h.chainTxID,
		Error:     *err,
	}

	if *err == nil {
		h.persistent <- persistentWithdraw
	} else {
		h.notif <- persistentWithdraw
	}
}

func (h *withdrawHandler) exec(ctx context.Context) error {
	h.newState = h.State

	var err error
	defer h.final(ctx, &err)

	if err = h.checkTransfer(ctx); err != nil {
		return err
	}

	return nil
}
