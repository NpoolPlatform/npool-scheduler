package types

import (
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	OrderUserReqs       []*orderusermwpb.OrderUserReq
	PowerRentalOrderReq *powerrentalordermwpb.PowerRentalOrderReq
}
