package notif

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	schedorderpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order"
	basenotif "github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/unlockaccount/types"
)

type handler struct{}

func NewNotif() basenotif.Notify {
	return &handler{}
}

func (p *handler) notifyPaid(order *types.PersistentOrder) error {
	return pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		return publisher.Update(
			basetypes.MsgID_OrderPaidReq.String(),
			nil,
			nil,
			nil,
			&schedorderpb.OrderInfo{
				AppID:            order.AppID,
				UserID:           order.UserID,
				OrderID:          order.OrderID,
				GoodType:         order.GoodType,
				Units:            order.Units,
				PaymentAmountUSD: order.PaymentAmountUSD,
				Payments: func() (payments []*schedorderpb.PaymentInfo) {
					for _, _payment := range order.PaymentBalances {
						payments = append(payments, &schedorderpb.PaymentInfo{
							CoinTypeID:  _payment.CoinTypeID,
							Amount:      _payment.Amount,
							PaymentType: schedorderpb.PaymentType_PayWithBalance,
						})
					}
					for _, _payment := range order.PaymentTransfers {
						payments = append(payments, &schedorderpb.PaymentInfo{
							CoinTypeID:  _payment.CoinTypeID,
							Amount:      _payment.Amount,
							PaymentType: schedorderpb.PaymentType_PayWithTransfer,
						})
					}
					return
				}(),
			},
		)
	})
}

func (p *handler) Notify(ctx context.Context, order interface{}, retry chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}
	if err := p.notifyPaid(_order); err != nil {
		retry1.Retry(_order.EntID, _order, retry)
		return err
	}
	return nil
}
