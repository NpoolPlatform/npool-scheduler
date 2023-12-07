package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

type OrderReward struct {
	AppID   string
	UserID  string
	OrderID uint32
	Amount  string
	Extra   string
}

type PersistentGood struct {
	*goodmwpb.Good
	OrderRewards    []*OrderReward
	BenefitResult   basetypes.Result
	BenefitMessage  string
	BenefitOrderIDs []uint32
	Error           error
}
