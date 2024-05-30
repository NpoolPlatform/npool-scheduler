package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
)

type CoinReward struct {
	CoinTypeID              string
	Amount                  string
	NextStartRewardAmount   string
	GoodBenefitAccountID    string
	GoodBenefitAddress      string
	UserBenefitHotAccountID string
	UserBenefitHotAddress   string
	Extra                   string
}

type PersistentPowerRental struct {
	*powerrentalmwpb.PowerRental
	BenefitOrderIDs  []uint32
	CoinRewards      []*CoinReward
	BenefitTimestamp uint32
	BenefitResult    basetypes.Result
	BenefitMessage   string
	Error            error
}

type FeedPowerRental struct {
	*powerrentalmwpb.PowerRental
	TriggerBenefitTimestamp uint32
}

type TriggerCond struct {
	GoodIDs  []string
	RewardAt uint32
}

func (c *TriggerCond) ContainGoodID(goodID string) bool {
	for _, id := range c.GoodIDs {
		if id == goodID {
			return true
		}
	}
	return false
}
