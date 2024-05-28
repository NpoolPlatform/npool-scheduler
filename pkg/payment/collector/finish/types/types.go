package types

import (
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
)

type PersistentAccount struct {
	*paymentaccountmwpb.Account
	Error error
}
