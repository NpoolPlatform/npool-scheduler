package types

import (
	ledgerwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
)

type PersistentWithdrawReviewNotify struct {
	Withdraws []*ledgerwithdrawmwpb.Withdraw
}
