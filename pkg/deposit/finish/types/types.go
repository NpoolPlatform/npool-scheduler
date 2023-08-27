package types

import (
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
)

type PersistentAccount struct {
	*depositaccmwpb.Account
	CollectOutcoming *string
	Error            error
}
