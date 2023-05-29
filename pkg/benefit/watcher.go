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

func nextBenefitAt() time.Time {
	now := time.Now()
	nowSec := now.Unix()
	benefitSeconds := int64(benefitInterval.Seconds())
	nextSec := (nowSec + benefitSeconds) / benefitSeconds * benefitSeconds
	return now.Add(time.Duration(nextSec-nowSec) * time.Second)
}

func delay() {
	start := nextBenefitAt()
	logger.Sugar().Infow("delay", "startAfter", time.Until(start).Seconds(), "start", start)
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

type bookKeepingData struct {
	GoodID   string
	Amount   string
	DateTime uint32
}

var bookKeepingTrigger chan *bookKeepingData

func processBookKeepingGood(ctx context.Context, data *bookKeepingData) {
	good, err := goodmwcli.GetGood(ctx, data.GoodID)
	if err != nil {
		logger.Sugar().Errorw(
			"processBookKeepingGood",
			"Data", data,
			"Error", err,
		)
		return
	}

	state := newState()

	g := newGood(good)
	g.LastBenefitAmount = data.Amount
	if err := state.BookKeeping(ctx, g); err != nil {
		logger.Sugar().Errorw(
			"processBookKeepingGood",
			"Data", data,
			"Error", err,
		)
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
	bookKeepingTrigger := make(chan *bookKeepingData)

	for {
		select {
		case <-tickerWait.C:
			logger.Sugar().Infow(
				"Watch",
				"State", "processWaitGoods ticker start",
			)
			processWaitGoods(ctx)
			logger.Sugar().Infow(
				"Watch",
				"State", "processWaitGoods ticker end",
			)
		case <-tickerTransferring.C:
			processTransferringGoods(ctx)
			processBookKeepingGoods(ctx)
		case data := <-bookKeepingTrigger:
			processBookKeepingGood(ctx, data)
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

func Redistribute(goodID, amount string, dateTime uint32) {
	go func() {
		bookKeepingTrigger <- &bookKeepingData{
			GoodID:   goodID,
			Amount:   amount,
			DateTime: dateTime,
		}
	}()
}
