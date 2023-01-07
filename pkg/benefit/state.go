package benefit

import (
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"

	"github.com/shopspring/decimal"
)

type State struct {
	Coins map[string]*coinmwpb.Coin
}

type Good struct {
	*goodmwpb.Good
	TodayRewardAmount         decimal.Decimal
	PlatformRewardAmount      decimal.Decimal
	UserRewardAmount          decimal.Decimal
	TechniqueServiceFeeAmount decimal.Decimal
}
