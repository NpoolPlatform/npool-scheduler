package types

import (
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	orderlockmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order/lock"
)

type PersistentFeeOrder struct {
	*feeordermwpb.FeeOrder
	LedgerStatements []*ledgerstatementmwpb.StatementReq
	CommissionLocks  map[string]*orderlockmwpb.OrderLock
}
