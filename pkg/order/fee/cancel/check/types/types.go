package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentPowerRentalOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	NewPaymentState *ordertypes.PaymentState
}
