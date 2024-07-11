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
}

type PersistentFeeOrder struct {
	*feeordermwpb.FeeOrder
	XPaymentTransfers []*XPaymentTransfer
	IncomingExtra     string
}
