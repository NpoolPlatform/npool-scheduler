package executor

import (
	"context"
	"fmt"

	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, payment interface{}, persistent, notif, done chan interface{}) error {
	_payment, ok := payment.(*paymentmwpb.Payment)
	if !ok {
		return fmt.Errorf("invalid payment")
	}
	h := &paymentHandler{
		Payment:    _payment,
		persistent: persistent,
		notif:      notif,
		done:       done,
	}
	return h.exec(ctx)
}
