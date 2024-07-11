package types

import (
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
)

type PersistentPayment struct {
	*paymentmwpb.Payment
	XLedgerLockID *string
}
