package executor

import (
	"context"
	"fmt"

	ledgerwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, withdraws interface{}, persistent, notif, done chan interface{}) error {
	_withdraws, ok := withdraws.([]*ledgerwithdrawmwpb.Withdraw)
	if !ok {
		return fmt.Errorf("invalid withdraws")
	}
	h := &withdrawReviewNotifyHandler{
		withdraws:  _withdraws,
		persistent: persistent,
		notif:      notif,
		done:       done,
	}
	return h.exec(ctx)
}
