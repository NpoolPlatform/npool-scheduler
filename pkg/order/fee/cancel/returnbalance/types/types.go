package types

import (
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
)

type Payment struct {
	CoinTypeID string
	Amount     string
	SpentExtra string
}

const (
	Ignore  = 0
	Unlock  = 1
	Unspend = 2
)

type PaymentOp = int

type PersistentOrder struct {
	*powerrentalordermwpb.PowerRentalOrder
	Payments  []*Payment
	PaymentOp PaymentOp
}
