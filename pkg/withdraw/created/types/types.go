package types

import (
	reviewtypes "github.com/NpoolPlatform/message/npool/basetypes/review/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
)

type PersistentWithdraw struct {
	*withdrawmwpb.Withdraw
	ReviewTrigger reviewtypes.ReviewTriggerType
	Error         error
}
