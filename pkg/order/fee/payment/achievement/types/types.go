package types

import (
	orderstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement/order"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
)

type PersistentOrder struct {
	*feeordermwpb.FeeOrder
	OrderStatements []*orderstatementmwpb.StatementReq
}
