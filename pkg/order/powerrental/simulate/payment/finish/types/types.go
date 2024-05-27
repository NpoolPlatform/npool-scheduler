package types

import (
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
)

type PersistentAccount struct {
	*payaccmwpb.Account
	Error error
}
