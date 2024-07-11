package types

import (
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
)

type CommissionRevoke struct {
	LockID       string
	IOExtra      string
	StatementIDs []string
}

type PersistentFeeOrder struct {
	*feeordermwpb.FeeOrder
	CommissionRevokes []*CommissionRevoke
}
