package types

import (
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
)

type PersistentAccount struct {
	*depositaccmwpb.Account
	CollectAmount          string
	FeeAmount              string
	DepositAccountID       string
	DepositAddress         string
	CollectAccountID       string
	CollectAddress         string
	CollectingTIDCandidate *string
	Error                  error
}
