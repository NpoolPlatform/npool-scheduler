package types

import (
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentPowerRentalOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
}
