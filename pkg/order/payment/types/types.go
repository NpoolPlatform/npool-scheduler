package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"

	"github.com/shopspring/decimal"
)

type PersistentOrder struct {
	*ordermwpb.Order
	PaymentBalance  decimal.Decimal
	NewOrderState   ordertypes.OrderState
	NewPaymentState ordertypes.PaymentState
}
