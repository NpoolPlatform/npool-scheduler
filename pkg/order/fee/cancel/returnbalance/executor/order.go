package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/returnbalance/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent chan interface{}
	notif      chan interface{}
	done       chan interface{}
	payments   []*types.Payment
	paymentOp  types.PaymentOp
}

func (h *orderHandler) constructPayments() error {
	switch h.OrderType {
	case ordertypes.OrderType_Offline:
		fallthrough //nolint
	case ordertypes.OrderType_Airdrop:
		return nil
	}

	switch h.CancelState {
	case ordertypes.OrderState_OrderStateWaitPayment:
		fallthrough //nolint
	case ordertypes.OrderState_OrderStatePaymentTimeout:
		h.paymentOp = types.Unlock
	case ordertypes.OrderState_OrderStatePaid:
		fallthrough //nolint
	case ordertypes.OrderState_OrderStateInService:
		h.paymentOp = types.Unspend
	default:
		return nil
	}

	for _, paymentTransfer := range h.PaymentTransfers {
		if _, err := decimal.NewFromString(paymentTransfer.Amount); err != nil {
			return wlog.WrapError(err)
		}
		h.payments = append(h.payments, &types.Payment{
			CoinTypeID: paymentTransfer.CoinTypeID,
			Amount:     paymentTransfer.Amount,
		})
	}
	for _, paymentBalance := range h.PaymentBalances {
		if _, err := decimal.NewFromString(paymentBalance.Amount); err != nil {
			return wlog.WrapError(err)
		}
		h.payments = append(h.payments, &types.Payment{
			CoinTypeID: paymentBalance.CoinTypeID,
			Amount:     paymentBalance.Amount,
		})
	}
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"Payments", h.payments,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder: h.PowerRentalOrder,
		Payments:         h.payments,
		PaymentOp:        h.paymentOp,
	}
	if len(h.payments) > 0 && h.paymentOp == types.Unspend {
		ioExtra := fmt.Sprintf(
			`{"AppID":"%v","UserID":"%v","OrderID":"%v","CancelOrder":true}`,
			h.AppID,
			h.UserID,
			h.EntID,
		)
		persistentOrder.SpentExtra = ioExtra
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

func (h *orderHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if err = h.constructPayments(); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
