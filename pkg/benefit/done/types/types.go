package types

import (
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

type PersistentGood struct {
	*goodmwpb.Good
	NextStartRewardAmount string
	BenefitOrderIDs       []string
	BenefitMessage        string
}
