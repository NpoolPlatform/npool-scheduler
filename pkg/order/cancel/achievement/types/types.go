package types

import (
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	orderlockmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order/orderlock"
)

type PersistentOrder struct {
	*ordermwpb.Order
	LedgerStatements []*ledgerstatementmwpb.StatementReq
	CommissionLocks  map[string]*orderlockmwpb.OrderLock
}
