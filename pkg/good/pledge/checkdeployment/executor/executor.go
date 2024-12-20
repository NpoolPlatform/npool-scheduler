package executor

import (
	"context"
	"fmt"

	goodpledgemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/pledge"

	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, pledge interface{}, persistent, notif, done chan interface{}) error {
	_pledge, ok := pledge.(*goodpledgemwpb.Pledge)
	if !ok {
		return fmt.Errorf("invalid good pledge")
	}

	h := &pledgeHandler{
		Pledge:     _pledge,
		persistent: persistent,
		notif:      notif,
		done:       done,
	}
	return h.exec(ctx)
}
