package benefit

import (
	"context"
	"fmt"
	"os"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/good"

	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
)

var benefitInterval = 2 * time.Minute

const secondsPerDay = timedef.SecondsPerDay

func interval() time.Duration {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		return duration
	}
	return benefitInterval
}

func tomorrowStart() time.Time {
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, now.Location())
}

func delay() {
	start := tomorrowStart()
	if time.Until(start) > benefitInterval {
		start = time.Now().Add(benefitInterval)
	}

	logger.Sugar().Infow("delay", "startAfter", time.Until(start))

	<-time.After(time.Until(start))
}

func processWaitGoods(ctx context.Context) {
	offset := int32(0)
	limit := int32(100)
	state := newState()

	for {
		goods, _, err := goodmwcli.GetGoods(ctx, &goodmgrpb.Conds{
			BenefitState: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(goodmgrpb.BenefitState_BenefitWait),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("processGoods", "Offset", offset, "Limit", limit, "Error", err)
			return
		}
		if len(goods) == 0 {
			return
		}

		for _, good := range goods {
			if good.StartAt > uint32(time.Now().Unix()) {
				continue
			}

			g := &Good{
				good,
				decimal.NewFromInt(0),
				decimal.NewFromInt(0),
				decimal.NewFromInt(0),
				decimal.NewFromInt(0),
			}

			if err := state.CalculateReward(ctx, g); err != nil {
				logger.Sugar().Errorw("processGoods", "GoodID", g.ID, "Error", err)
				continue
			}
			if err := state.CalculateTechniqueServiceFee(ctx, g); err != nil {
				logger.Sugar().Errorw("processGoods", "GoodID", g.ID, "Error", err)
				continue
			}

			logger.Sugar().Infow("processGoods",
				"GoodID", g.ID,
				"GoodName", g.Title,
				"TodayRewardAmount", g.TodayRewardAmount,
				"PlatformRewardAmount", g.PlatformRewardAmount,
				"UserRewardAmount", g.UserRewardAmount,
				"TechniqueServiceFeeAmount", g.TechniqueServiceFeeAmount,
			)

			if err := state.TransferReward(ctx, g); err != nil {
				logger.Sugar().Errorw("processGoods", "GoodID", g.ID, "Error", err)
			}
		}

		offset += limit
	}
}

func processTransferringGoods(ctx context.Context) {
}

func processBookKeepingGoods(ctx context.Context) {
}

func Watch(ctx context.Context) {
	benefitInterval = interval()
	logger.Sugar().Infow("benefit", "intervalSeconds", benefitInterval)

	delay()

	ticker := time.NewTicker(benefitInterval)
	for {
		processWaitGoods(ctx)
		processTransferringGoods(ctx)
		processBookKeepingGoods(ctx)
		<-ticker.C
	}
}
