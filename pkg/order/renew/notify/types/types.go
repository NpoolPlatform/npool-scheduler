package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	orderrenewpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order/renew"
)

type PersistentOrder struct {
	*ordermwpb.Order
	*orderrenewpb.MsgOrderChildsRenewReq
	NewRenewState ordertypes.OrderRenewState
}
