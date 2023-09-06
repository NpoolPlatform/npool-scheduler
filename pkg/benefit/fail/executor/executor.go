package executor

import (
	"context"
	"fmt"

	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, good interface{}, persistent, notif, done chan interface{}) error {
	_good, ok := good.(*goodmwpb.Good)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	h := &goodHandler{
		Good:       _good,
		persistent: persistent,
	}
	return h.exec(ctx)
}
