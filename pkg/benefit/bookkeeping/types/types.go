package types

import (
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
	Error              error
}
