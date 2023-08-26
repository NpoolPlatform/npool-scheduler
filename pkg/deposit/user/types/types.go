package types

import (
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
)

type PersistentAccount struct {
	*depositaccmwpb.Account
	DepositAmount string
	Extra         string
	Error         error
}
