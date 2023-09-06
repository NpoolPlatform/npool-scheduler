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
	FeeAmount               string
	BenefitTimestamp        uint32
	Extra                   string
	BenefitResult           basetypes.Result
	BenefitMessage          string
	Error                   error
}
