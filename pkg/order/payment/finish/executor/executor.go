package executor

import (
	"context"
	"fmt"

	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, account interface{}, persistent, notif, done chan interface{}) error {
	_account, ok := account.(*payaccmwpb.Account)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	h := &accountHandler{
		Account:    _account,
		persistent: persistent,
		done:       done,
	}
	return h.exec(ctx)
}
