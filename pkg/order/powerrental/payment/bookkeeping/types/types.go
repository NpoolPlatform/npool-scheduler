package types

import (
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type XPaymentTransfer struct {
	PaymentTransferID     string
	CoinTypeID            string
	AccountID             string
	PaymentAccountBalance string
	IncomingAmount        *string
	Amount                string
	StartAmount           string
	FinishAmount          string
	IncomingExtra         string
	OutcomingExtra        string
}

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	XPaymentTransfers []*XPaymentTransfer
	Error             error
}
