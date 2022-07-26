package benefit

import (
	"context"
	"time"

	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"
	goodspb "github.com/NpoolPlatform/message/npool/cloud-hashing-goods"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"
	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
)

type gp struct {
	good           *goodspb.GoodInfo
	benefitAccount *billingpb.CoinAccountInfo
	setting        *billingpb.CoinSetting
	coin           *coininfopb.CoinInfo
}

func (g *gp) dailyProfit(ctx context.Context) error {
	return nil
}

func (g *gp) processOrder(ctx context.Context, order *orderpb.Order, timestamp time.Time) error {
	return nil
}
