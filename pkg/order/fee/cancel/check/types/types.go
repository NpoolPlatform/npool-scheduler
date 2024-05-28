package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
)

type PersistentFeeOrder struct {
	*feeordermwpb.FeeOrder
	NewPaymentState *ordertypes.PaymentState
}
