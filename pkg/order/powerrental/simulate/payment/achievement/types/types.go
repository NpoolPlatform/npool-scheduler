package types

import (
	orderstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement/order"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	OrderStatements []*orderstatementmwpb.StatementReq
}
