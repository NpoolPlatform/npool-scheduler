package types

import (
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	AppGoodStockID             string
	AppGoodStockLockID         string
	ExistOrderCompletedHistory bool
}
