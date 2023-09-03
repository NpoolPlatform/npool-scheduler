package executor

import (
	"context"
	"fmt"

	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, notif interface{}, retry, persistent, notif1 chan interface{}) error {
	_notif, ok := notif.(*notifmwpb.Notif)
	if !ok {
		return fmt.Errorf("invalid notif")
	}
	h := &notifHandler{
		Notif:      _notif,
		persistent: persistent,
	}
	return h.exec(ctx)
}
