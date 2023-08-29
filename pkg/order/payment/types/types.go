package types

import (
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"

	"github.com/shopspring/decimal"
)

type PersistentOrder struct {
	*ordermwpb.Order
	PaymentBalance         decimal.Decimal
	NewOrderState          ordertypes.OrderState
	NewPaymentState        ordertypes.PaymentState
	IncomingAmount         *string
	IncomingExtra          string
	TransferOutcomingExtra string
	BalanceOutcomingExtra  string
	AchievementStatements  []*achievementstatementmwpb.StatementReq
	CommissionStatements   []*ledgerstatementmwpb.StatementReq
	Error                  error
}
