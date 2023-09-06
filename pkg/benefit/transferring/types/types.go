package types

import (
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

type PersistentGood struct {
	*goodmwpb.Good
	TransferToPlatform      bool
	ToPlatformAmount        string
	NewBenefitState         goodtypes.BenefitState
	UserBenefitHotAccountID string
	UserBenefitHotAddress   string
	PlatformColdAccountID   string
	PlatformColdAddress     string
	FeeAmount               string
	NextStartAmount         string
	Extra                   string
	BenefitResult           basetypes.Result
	BenefitMessage          string
	Error                   error
}
