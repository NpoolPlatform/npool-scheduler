package benefit

import (
	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"

	"github.com/shopspring/decimal"
)

type State struct {
	Coins            map[string]*coinmwpb.Coin
	PlatformAccounts map[string]map[accountmgrpb.AccountUsedFor]*pltfaccmwpb.Account // map[CoinTypeID]map[UsedFor]Account
}

func newState() *State {
	return &State{
		Coins:            map[string]*coinmwpb.Coin{},
		PlatformAccounts: map[string]map[accountmgrpb.AccountUsedFor]*pltfaccmwpb.Account{},
	}
}

type Good struct {
	*goodmwpb.Good
	TodayRewardAmount         decimal.Decimal
	PlatformRewardAmount      decimal.Decimal
	UserRewardAmount          decimal.Decimal
	TechniqueServiceFeeAmount decimal.Decimal
}
