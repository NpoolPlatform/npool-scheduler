package executor

import (
	"context"
	"fmt"

	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, benefits interface{}, persistent, notif, done chan interface{}) error {
	_benefits, ok := benefits.([]*notifbenefitmwpb.GoodBenefit)
	if !ok {
		return fmt.Errorf("invalid goodbenefit")
	}
	h := &benefitHandler{
		benefits:   _benefits,
		persistent: persistent,
		notif:      notif,
		done:       done,
	}
	return h.exec(ctx)
}
