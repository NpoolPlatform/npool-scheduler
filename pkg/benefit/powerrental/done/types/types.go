package types

import (
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
)

type CoinNextReward struct {
	CoinTypeID            string
	NextRewardStartAmount string
	BenefitMessage        string
}

type PersistentGood struct {
	*powerrentalmwpb.PowerRental
	CoinNextRewards []*CoinNextReward
	BenefitOrderIDs []uint32
}
