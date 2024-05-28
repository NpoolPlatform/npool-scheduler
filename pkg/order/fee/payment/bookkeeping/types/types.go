package types

import (
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"

	"github.com/shopspring/decimal"
)

type XPaymentTransfer struct {
	PaymentTransferID     string
	CoinTypeID            string
	AccountID             string
	PaymentAccountBalance string
	IncomingAmount        *string
	Amount                decimal.Decimal
	StartAmount           decimal.Decimal
	FinishAmount          string
	IncomingExtra         string
	OutcomingExtra        string
}

type PersistentOrder struct {
	*feeordermwpb.FeeOrder
	XPaymentTransfers []*XPaymentTransfer
	Error             error
}
