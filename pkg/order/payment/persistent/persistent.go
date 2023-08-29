package persistent

import (
	"context"
	"fmt"

	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, order interface{}, retry, notif chan interface{}) error {
	_, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	/*
		const timeoutSeconds = 10
		sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
			WaitResult:     true,
			RequestTimeout: timeoutSeconds,
		})
		p.withUpdateStock(sagaDispose, _order)
		p.withUpdateOrder(sagaDispose, _order)
		if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
			retry1.Retry(ctx, _order, retry)
			return err
		}
	*/

	return nil
}
