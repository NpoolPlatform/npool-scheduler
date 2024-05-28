package types

import (
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
)

type PersistentOrder struct {
	*feeordermwpb.FeeOrder
	PaymentAccountIDs []uint32
}
