package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	NewRenewState      ordertypes.OrderRenewState
	NextRenewNotifyAt  uint32
	CreateOutOfGas     bool
	FeeEndAt           uint32
	OutOfGasEntID      string
	FinishOutOfGas     bool
	OutOfGasFinishedAt uint32
}
