package types

import (
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type CommissionRevoke struct {
	LockID       string
	IOExtra      string
	StatementIDs []string
}

type PersistentPowerRentalOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	CommissionRevokes []*CommissionRevoke
}
