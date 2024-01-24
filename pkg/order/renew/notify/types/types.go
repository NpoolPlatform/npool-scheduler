package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type FeeDeduction struct {
	CoinName    string
	CoinUnit    string
	USDCurrency string
	Amount      string
}

type PersistentOrder struct {
	*ordermwpb.Order
	NewRenewState ordertypes.OrderRenewState
	FeeDeductions []*FeeDeduction
}
