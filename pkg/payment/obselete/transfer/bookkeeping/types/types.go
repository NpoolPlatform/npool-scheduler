package types

import (
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
)

type PersistentPayment struct {
	*paymentmwpb.Payment
	Statements       []*ledgerstatementmwpb.StatementReq
	PaymentTransfers []*paymentmwpb.PaymentTransferReq
}
