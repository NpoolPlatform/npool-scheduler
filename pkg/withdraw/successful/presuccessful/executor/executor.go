package executor

import (
	"context"
	"fmt"

	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, withdraw interface{}, retry, persistent, notif chan interface{}) error {
	_withdraw, ok := withdraw.(*withdrawmwpb.Withdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	h := &withdrawHandler{
		Withdraw:   _withdraw,
		persistent: persistent,
	}
	return h.exec(ctx)
}
