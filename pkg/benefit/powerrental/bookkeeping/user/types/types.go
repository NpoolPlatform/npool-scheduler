package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
)

type CoinReward struct {
	CoinTypeID string
	Amount     string
}

type OrderReward struct {
	AppID        string
	UserID       string
	OrderID      string
	Extra        string
	FirstBenefit bool
	CoinRewards  []*CoinReward
}

type PersistentGood struct {
	*powerrentalmwpb.PowerRental
	OrderRewards    []*OrderReward
	BenefitResult   basetypes.Result
	BenefitOrderIDs []uint32
	Error           error
}
