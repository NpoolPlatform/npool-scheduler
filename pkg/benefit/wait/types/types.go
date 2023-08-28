package types

import (
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

type PersistentGood struct {
	*goodmwpb.Good
	BenefitOrderIDs         []string
	GoodBenefitAccountID    string
	GoodBenefitAddress      string
	UserBenefitHotAccountID string
	UserBenefitHotAddress   string
	TodayRewardAmount       string
	FeeAmount               string
	BenefitTimestamp        uint32
	RewardTID               *string
	Extra                   string
	Error                   error
}
