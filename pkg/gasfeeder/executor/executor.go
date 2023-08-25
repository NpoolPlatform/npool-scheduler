package executor

import (
	"context"
	"fmt"

	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct {
	baseexecutor.Executor
}

func NewExecutor(ctx context.Context, cancel context.CancelFunc, persistent, notif chan interface{}) baseexecutor.Executor {
	h := &handler{}
	return baseexecutor.NewExecutor(ctx, cancel, persistent, notif, h)
}

func (e *handler) Exec(ctx context.Context, coin interface{}) error {
	_coin, ok := coin.(*coinmwpb.Coin)
	if !ok {
		return fmt.Errorf("invalid coin")
	}

	h := &coinHandler{
		Coin:       _coin,
		persistent: e.Persistent(),
		notif:      e.Notif(),
	}
	return h.exec(ctx)
}
