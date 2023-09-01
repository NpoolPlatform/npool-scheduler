package types

import (
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type PersistentOrder struct {
	*ordermwpb.Order
	AchievementStatements []*achievementstatementmwpb.StatementReq
}
