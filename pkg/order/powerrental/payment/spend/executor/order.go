package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/spend/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent    chan interface{}
	done          chan interface{}
	balanceAmount decimal.Decimal
	balances      []*ledgermwpb.LockBalancesRequest_XBalance
}

func (h *orderHandler) resolveMultiCoinPayments() {
	if !h.MultiPaymentCoins {
		return
	}
	for _, amount := range h.PaymentAmounts {
		h.balances = append(h.balances, &ledgermwpb.LockBalancesRequest_XBalance{
			CoinTypeID: amount.CoinTypeID,
			Amount:     amount.Amount,
		})
	}
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"BalanceAmount", h.balanceAmount,
			"Balances", h.balances,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:              h.Order,
		OrderBalanceAmount: h.balanceAmount.String(),
		OrderBalanceLockID: h.LedgerLockID,
		Balances:           h.balances,
	}
	if h.balanceAmount.Cmp(decimal.NewFromInt(0)) > 0 || len(h.balances) > 0 {
		persistentOrder.BalanceExtra = fmt.Sprintf(
			`{"PaymentID":"%v","OrderID": "%v","FromBalance":true}`,
			h.PaymentID,
			h.EntID,
		)
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)
	if h.balanceAmount, err = decimal.NewFromString(h.BalanceAmount); err != nil {
		return err
	}
	h.resolveMultiCoinPayments()
	return nil
}
