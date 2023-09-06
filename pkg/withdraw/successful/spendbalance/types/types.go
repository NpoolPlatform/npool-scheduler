package types

import (
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
)

type PersistentWithdraw struct {
	*withdrawmwpb.Withdraw
	LockedBalanceAmount string
	WithdrawFeeAmount   string
}
