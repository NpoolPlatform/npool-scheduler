package types

import (
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type PersistentOrder struct {
	*ordermwpb.Order
	OrderBalanceAmount string
	BalanceExtra       string
	OrderBalanceLockID string
}
