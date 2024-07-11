package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
)

type CoinNextReward struct {
	CoinTypeID            string
	NextRewardStartAmount string
	BenefitMessage        string
}

type PersistentGood struct {
	*powerrentalmwpb.PowerRental
	BenefitResult   basetypes.Result
	CoinNextRewards []*CoinNextReward
	BenefitOrderIDs []uint32
	Error           error
}
