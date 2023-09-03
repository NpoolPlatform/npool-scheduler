package executor

import (
	"context"
	"fmt"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/bookkept/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent    chan interface{}
	balanceAmount decimal.Decimal
}

func (h *orderHandler) final() {
	persistentOrder := &types.PersistentOrder{
		Order:         h.Order,
		BalanceAmount: h.balanceAmount.String(),
	}
	if h.balanceAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		persistentOrder.BalanceExtra = fmt.Sprintf(
			`{"PaymentID":"%v","OrderID": "%v","FromBalance":true}`,
			h.PaymentID,
			h.ID,
		)
	}
	h.persistent <- persistentOrder
}

func (h *orderHandler) exec(ctx context.Context) error { //nolint
	var err error
	if h.balanceAmount, err = decimal.NewFromString(h.TransferAmount); err != nil {
		return err
	}
	h.final()
	return nil
}
