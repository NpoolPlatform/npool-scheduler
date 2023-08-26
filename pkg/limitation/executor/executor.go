package executor

import (
	"context"
	"fmt"

	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, coin interface{}, retry, persistent, notif chan interface{}) error {
	_coin, ok := coin.(*coinmwpb.Coin)
	if !ok {
		return fmt.Errorf("invalid coin")
	}

	h := &coinHandler{
		Coin:       _coin,
		persistent: persistent,
		notif:      notif,
	}
	return h.exec(ctx)
}
