package types

import (
	fractionwithdrawalmwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/fractionwithdrawal"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	FractionWithdrawalReqs []*fractionwithdrawalmwpb.FractionWithdrawalReq
	PowerRentalOrderReq    *powerrentalordermwpb.PowerRentalOrderReq
}
