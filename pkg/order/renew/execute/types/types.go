package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	appgoodstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/stock"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type OrderReq struct {
	*ordermwpb.OrderReq
	Balances []*ledgermwpb.LockBalancesRequest_XBalance
	Stock    *appgoodstockmwpb.LocksRequest_XStock
}

type PersistentOrder struct {
	*ordermwpb.Order
	InsufficientBalance bool
	OrderReqs           []*OrderReq
	NewRenewState       ordertypes.OrderRenewState
}
