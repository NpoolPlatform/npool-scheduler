package types

import (
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
)

type PersistentOrder struct {
	*feeordermwpb.FeeOrder
	// ID of payment table but not account table
	PaymentAccountIDs []uint32
}
