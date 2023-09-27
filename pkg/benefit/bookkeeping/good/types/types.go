package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

type PersistentGood struct {
	*goodmwpb.Good
	TotalRewardAmount  string
	UnsoldRewardAmount string
	TechniqueFeeAmount string
	StatementExist     bool
	BenefitResult      basetypes.Result
	BenefitMessage     string
	Error              error
}
