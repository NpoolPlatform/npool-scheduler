package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

type OrderReward struct {
	AppID  string
	UserID string
	Amount string
	Extra  string
}

type PersistentGood struct {
	*goodmwpb.Good
	OrderRewards       []*OrderReward
	TotalRewardAmount  string
	UnsoldRewardAmount string
	TechniqueFeeAmount string
	UserRewardAmount   string
	StatementExist     bool
	BenefitResult      basetypes.Result
	BenefitMessage     string
	Error              error
}
