package executor

import (
	"context"
	"fmt"

	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, good interface{}, persistent, notif, done chan interface{}) error {
	_good, ok := good.(*powerrentalmwpb.PowerRental)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	h := &goodHandler{
		PowerRental: _good,
		persistent:  persistent,
		notif:       notif,
		done:        done,
	}
	return h.exec(ctx)
}
