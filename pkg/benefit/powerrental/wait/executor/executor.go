package executor

import (
	"context"
	"fmt"

	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
	common "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/wait/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/wait/types"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, good interface{}, persistent, notif, done chan interface{}) error {
	_good, ok := good.(*types.FeedPowerRental)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	h := &goodHandler{
		FeedPowerRental: _good,
		Handler:         common.NewHandler(),
		persistent:      persistent,
		notif:           notif,
		done:            done,
	}
	return h.exec(ctx)
}
