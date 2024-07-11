package notif

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	basenotif "github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/renew/notify/types"
)

type handler struct{}

func NewNotif() basenotif.Notify {
	return &handler{}
}

func (p *handler) notifyChildsRenew(order *types.PersistentOrder) error {
	return pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		return publisher.Update(
			basetypes.MsgID_OrderChildsRenewReq.String(),
			nil,
			nil,
			nil,
			order.MsgOrderChildsRenewReq,
		)
	})
}

func (p *handler) Notify(ctx context.Context, order interface{}, retry chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}
	if err := p.notifyChildsRenew(_order); err != nil {
		retry1.Retry(_order.EntID, _order, retry)
		return err
	}
	return nil
}
