package executor

import (
	"context"
	"fmt"

	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, account interface{}, persistent, notif, done chan interface{}) error {
	_account, ok := account.(*depositaccmwpb.Account)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	h := &accountHandler{
		Account:    _account,
		persistent: persistent,
		notif:      notif,
		done:       done,
	}
	return h.exec(ctx)
}
