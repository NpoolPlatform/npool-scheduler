package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	fractionmwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/fraction"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	FractionReqs []*fractionmwpb.FractionReq
	NextState    *ordertypes.OrderState
}
