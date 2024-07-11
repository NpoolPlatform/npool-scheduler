package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderrenewpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order/renew"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	*orderrenewpb.MsgOrderChildsRenewReq
	NewRenewState ordertypes.OrderRenewState
}
