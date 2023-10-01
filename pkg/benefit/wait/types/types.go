package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
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
	TodayUnitRewardAmount   string
	NextStartRewardAmount   string
	LastUnitRewardAmount    string
	FeeAmount               string
	BenefitTimestamp        uint32
	Extra                   string
	BenefitResult           basetypes.Result
	BenefitMessage          string
	Transferrable           bool
	Error                   error
}

type FeedGood struct {
	*goodmwpb.Good
	TriggerBenefitTimestamp uint32
}

type TriggerCond struct {
	GoodIDs  []string
	RewardAt uint32
}

func (c *TriggerCond) ContainGoodID(goodID string) bool {
	if len(c.GoodIDs) == 0 {
		return true
	}
	for _, id := range c.GoodIDs {
		if id == goodID {
			return true
		}
	}
	return false
}
