package benefit

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/good"

	commonpb "github.com/NpoolPlatform/message/npool"
)

var (
	benefitInterval = 24 * time.Hour
	checkInterval   = 10 * time.Minute
)

func prepareInterval() {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		benefitInterval = duration
	}
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_CHECK_INTERVAL_SECONDS"))); err == nil {
		checkInterval = duration
	}
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

func processWaitGoods(ctx context.Context) { //nolint
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
			logger.Sugar().Errorw("processWaitGoods", "Offset", offset, "Limit", limit, "Error", err)
			return
		}
		if len(goods) == 0 {
			return
		}

		for _, good := range goods {
			if good.StartAt > uint32(time.Now().Unix()) {
				continue
			}

			timestamp1 := benefitTimestamp(uint32(time.Now().Unix()), benefitInterval)
			timestamp2 := benefitTimestamp(good.LastBenefitAt, benefitInterval)
			if timestamp1 == timestamp2 {
				continue
			}

			g := newGood(good)

			if err := state.CalculateReward(ctx, g); err != nil {
				logger.Sugar().Errorw("processWaitGoods", "GoodID", g.ID, "Error", err)
				continue
			}
			if err := state.CalculateTechniqueServiceFee(ctx, g); err != nil {
				logger.Sugar().Errorw("processWaitGoods", "GoodID", g.ID, "Error", err)
				continue
			}
			if err := UpdateDailyReward(ctx, g); err != nil {
				logger.Sugar().Errorw("processWaitGoods", "GoodID", g.ID, "Error", err)
				continue
			}

			logger.Sugar().Infow("processWaitGoods",
				"GoodID", g.ID,
				"GoodName", g.Title,
				"TodayRewardAmount", g.TodayRewardAmount,
				"PlatformRewardAmount", g.PlatformRewardAmount,
				"UserRewardAmount", g.UserRewardAmount,
				"TechniqueServiceFeeAmount", g.TechniqueServiceFeeAmount,
			)

			if err := state.TransferReward(ctx, g); err != nil {
				logger.Sugar().Errorw("processWaitGoods", "GoodID", g.ID, "Error", err)
			}
		}

		offset += limit
	}
}

func processTransferringGoods(ctx context.Context) {
	offset := int32(0)
	limit := int32(100)
	state := newState()

	for {
		goods, _, err := goodmwcli.GetGoods(ctx, &goodmgrpb.Conds{
			BenefitState: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(goodmgrpb.BenefitState_BenefitTransferring),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("processTransferringGoods", "Offset", offset, "Limit", limit, "Error", err)
			return
		}
		if len(goods) == 0 {
			return
		}

		for _, good := range goods {
			g := newGood(good)

			if err := state.CheckTransfer(ctx, g); err != nil {
				logger.Sugar().Errorw("processTransferringGoods", "GoodID", g.ID, "Error", err)
			}
		}

		offset += limit
	}
}

func processBookKeepingGoods(ctx context.Context) {
	offset := int32(0)
	limit := int32(100)
	state := newState()

	for {
		goods, _, err := goodmwcli.GetGoods(ctx, &goodmgrpb.Conds{
			BenefitState: &commonpb.Int32Val{
				Op:    cruder.EQ,
				Value: int32(goodmgrpb.BenefitState_BenefitBookKeeping),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("processBookKeepingGoods", "Offset", offset, "Limit", limit, "Error", err)
			return
		}
		if len(goods) == 0 {
			return
		}

		for _, good := range goods {
			g := newGood(good)

			if err := state.BookKeeping(ctx, g); err != nil {
				logger.Sugar().Errorw("processBookKeepingGoods", "GoodID", g.ID, "Error", err)
			}
		}

		offset += limit
	}
}

func Watch(ctx context.Context) {
	prepareInterval()
	logger.Sugar().Infow(
		"benefit",
		"BenefitIntervalSeconds", benefitInterval,
		"CheckIntervalSeconds", checkInterval,
	)

	delay()
	processWaitGoods(ctx)

	tickerWait := time.NewTicker(benefitInterval)
	tickerTransferring := time.NewTicker(checkInterval)

	for {
		select {
		case <-tickerWait.C:
			processWaitGoods(ctx)
		case <-tickerTransferring.C:
			processTransferringGoods(ctx)
			processBookKeepingGoods(ctx)
		case <-ctx.Done():
			logger.Sugar().Infow(
				"Watch",
				"State", "Done",
				"Error", ctx.Err(),
			)
			return
		}
	}
}
