package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	InsufficientBalance bool
	FeeOrderReqs        []*feeordermwpb.FeeOrderReq
	NewRenewState       ordertypes.OrderRenewState
	LedgerLockID        string
	CreateOutOfGas      bool
	FeeEndAt            uint32
	OutOfGasEntID       *string
}
