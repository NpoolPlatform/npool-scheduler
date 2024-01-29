package types

import (
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type PersistentOrder struct {
	*ordermwpb.Order
	OrderBalanceAmount string
	BalanceExtra       string
	OrderBalanceLockID string
	MultiPaymentCoins  bool
	Balances           []*ledgermwpb.LockBalancesRequest_XBalance
}
