package executor

import (
	"context"
	"fmt"

	goodpowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"

	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, powerRental interface{}, persistent, notif, done chan interface{}) error {
	_powerRental, ok := powerRental.(*goodpowerrentalmwpb.PowerRental)
	if !ok {
		return fmt.Errorf("invalid good powerrental")
	}

	h := &powerRentalHandler{
		PowerRental: _powerRental,
		persistent:  persistent,
		notif:       notif,
		done:        done,
	}
	return h.exec(ctx)
}
