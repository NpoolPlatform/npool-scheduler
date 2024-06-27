package types

import (
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
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
	*feeordermwpb.FeeOrder
	Payments  []*Payment
	PaymentOp PaymentOp
}
