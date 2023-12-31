package types

import (
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
)

type PersistentOrder struct {
	*ordermwpb.Order
	PaymentAccountBalance string
	IncomingAmount        string
	IncomingExtra         string
	TransferAmount        string
	TransferExtra         string
	Error                 error
}
