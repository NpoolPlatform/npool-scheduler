package order

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	ordercli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"
	paymentmgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/payment"
	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
)

const secondsPerDay = uint32(24 * 60 * 60)

func orderExpired(ctx context.Context, order *orderpb.Order) (bool, error) {
	good, err := goodmwcli.GetGood(ctx, order.GoodID)
	if err != nil {
		return false, err
	}
	if good == nil {
		return false, nil
	}

	switch order.PaymentState {
	case paymentmgrpb.PaymentState_Wait:
		fallthrough // nolint
	case paymentmgrpb.PaymentState_Canceled:
		fallthrough // nolint
	case paymentmgrpb.PaymentState_TimeOut:
		return false, nil
	case paymentmgrpb.PaymentState_Done:
	default:
		return false, fmt.Errorf("invalid payment state")
	}

	if order.Start+uint32(good.DurationDays)*secondsPerDay >= uint32(time.Now().Unix()) {
		return false, nil
	}

	return true, nil
}

func processOrderExpiry(ctx context.Context, order *orderpb.Order) error {
	// TODO: will be remove when formal refactor order
	expired, err := orderExpired(ctx, order)
	if err != nil {
		return err
	}
	if !expired {
		return nil
	}

	if order.OrderState == ordermgrpb.OrderState_Expired {
		return nil
	}

	ostate := ordermgrpb.OrderState_Expired
	_, err = ordercli.UpdateOrder(ctx, &orderpb.OrderReq{
		ID:    &order.ID,
		State: &ostate,
	})

	if err != nil {
		return err
	}

	units, err := decimal.NewFromString(order.Units)
	if err != nil {
		return err
	}
	units = units.Neg()
	err = updateStock(ctx, order.GoodID, decimal.NewFromInt(0), units, decimal.NewFromInt(0))
	if err != nil {
		return err
	}

	return nil
}

func checkOrderExpiries(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	for {
		orders, _, err := ordercli.GetOrders(ctx, &orderpb.Conds{
			State: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(ordermgrpb.OrderState_InService),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("processOrders", "offset", offset, "limit", limit, "error", err)
			return
		}
		if len(orders) == 0 {
			return
		}

		for index, order := range orders {
			if err := processOrderExpiry(ctx, order); err != nil {
				logger.Sugar().Errorw("processOrders", "offset", offset, "index", index, "error", err)
				return
			}
		}

		offset += limit
	}
}
