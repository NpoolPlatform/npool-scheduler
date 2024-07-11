package types

import (
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
)

type PersistentFeeOrder struct {
	*feeordermwpb.FeeOrder
}
