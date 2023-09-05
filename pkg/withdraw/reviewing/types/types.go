package types

import (
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	reviewtypes "github.com/NpoolPlatform/message/npool/basetypes/review/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
)

type PersistentWithdraw struct {
	*withdrawmwpb.Withdraw
	NewWithdrawState ledgertypes.WithdrawState
	NewReviewState   reviewtypes.ReviewState
	NeedUpdateReview bool
	Error            error
}
