package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type OrderReq struct {
	*ordermwpb.OrderReq
	Balances []*ledgermwpb.LockBalancesRequest_XBalance
}

type PersistentOrder struct {
	*ordermwpb.Order
	OrderReqs     []*OrderReq
	NewRenewState ordertypes.OrderRenewState
}
