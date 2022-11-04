package order

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"

	orderstatepb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order/state"
	orderstatecli "github.com/NpoolPlatform/order-manager/pkg/client/state"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	commonpb "github.com/NpoolPlatform/message/npool"
)

const secondsPerDay = uint32(24 * 60 * 60)

func orderExpired(ctx context.Context, order *orderpb.Order) (bool, error) {
	payment, err := ordercli.GetOrderPayment(ctx, order.ID)
	if err != nil {
		return false, err
	}

	good, err := goodscli.GetGood(ctx, order.GoodID)
	if err != nil {
		return false, err
	}
	if good == nil {
		return false, nil
	}

	if payment == nil {
		return false, nil
	}

	switch payment.State {
	case orderconst.PaymentStateWait:
		fallthrough // nolint
	case orderconst.PaymentStateCanceled:
		fallthrough // nolint
	case orderconst.PaymentStateTimeout:
		return false, nil
	case orderconst.PaymentStateDone:
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

	state, err := orderstatecli.GetStateOnly(ctx, &orderstatepb.Conds{
		OrderID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.ID,
		},
	})
	if err != nil {
		return err
	}

	if state != nil && state.State == orderstatepb.EState_Expired {
		return nil
	}

	ostate := orderstatepb.EState_Expired
	_, err = orderstatecli.CreateState(ctx, &orderstatepb.StateReq{
		OrderID: &order.ID,
		State:   &ostate,
	})
	if err != nil {
		return err
	}

	err = updateStock(ctx, order.GoodID, 0, int32(order.Units)*-1)
	if err != nil {
		return err
	}

	return nil
}

func checkOrderExpiries(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	for {
		orders, err := ordercli.GetOrders(ctx, offset, limit)
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
