package types

import (
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	orderlockmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order/lock"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentPowerRentalOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	LedgerStatements []*ledgerstatementmwpb.StatementReq
	CommissionLocks  map[string]*orderlockmwpb.OrderLock
}
