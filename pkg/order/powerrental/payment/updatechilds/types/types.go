package types

import (
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type PersistentOrder struct {
	*ordermwpb.Order
	// Orders with child order which are paid with parent
	ChildOrders []*ordermwpb.Order
	Error       error
}
