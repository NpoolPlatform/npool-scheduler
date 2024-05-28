package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
)

type PersistentOrder struct {
	*feeordermwpb.FeeOrder
	NewOrderState   ordertypes.OrderState
	NewPaymentState *ordertypes.PaymentState
	Error           error
}
