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

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	commonpb "github.com/NpoolPlatform/message/npool"
	"github.com/shopspring/decimal"
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

func processWaitGoods(ctx context.Context, goodIDs []string) []string { //nolint
	offset := int32(0)
	limit := int32(100)
	state := newState()
	retryGoods := []string{}

	for {
		conds := &goodmgrpb.Conds{
			BenefitState: &commonpb.Int32Val{Op: cruder.EQ, Value: int32(goodmgrpb.BenefitState_BenefitWait)},
		}
		if len(goodIDs) > 0 {
			conds.IDs = &commonpb.StringSliceVal{Op: cruder.IN, Value: goodIDs}
		}
		goods, _, err := goodmwcli.GetGoods(ctx, conds, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("processWaitGoods", "Offset", offset, "Limit", limit, "Error", err)
			return []string{}
		}
		if len(goods) == 0 {
			return []string{}
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
				retryGoods = append(retryGoods, g.ID)
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

	return retryGoods
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

var bookKeepingTrigger = make(chan *bookKeepingData)

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
	state.ChangeState = false

	g := newGood(good)
	g.LastBenefitAmount = data.Amount
	g.LastBenefitAt = data.DateTime

	offset := int32(0)
	const limit = int32(100)

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID: &commonpb.StringVal{Op: cruder.EQ, Value: good.ID},
			State:  &commonpb.Uint32Val{Op: cruder.EQ, Value: uint32(ordermgrpb.OrderState_InService)},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw(
				"processBookKeepingGood",
				"Data", data,
				"Error", err,
			)
			return
		}
		if len(orders) == 0 {
			break
		}

		ords := []*ordermwpb.OrderReq{}
		for _, ord := range orders {
			if !benefitable(g.Good, ord, data.DateTime) {
				continue
			}
			_, err := decimal.NewFromString(ord.Units)
			if err != nil {
				logger.Sugar().Errorw(
					"processBookKeepingGood",
					"Data", data,
					"Error", err,
				)
				return
			}
			g.BenefitOrderIDs[ord.ID] = ord.PaymentID

			ords = append(ords, &ordermwpb.OrderReq{
				ID:            &ord.ID,
				PaymentID:     &ord.PaymentID,
				LastBenefitAt: &g.LastBenefitAt,
			})
		}

		if len(ords) > 0 {
			logger.Sugar().Infow(
				"processBookKeepingGood",
				"GoodID", good.ID,
				"UserRewardAmount", good.LastBenefitAmount,
				"UpdateOrders", len(ords),
				"LastBenefitAt", g.LastBenefitAt,
			)
			_, err := ordermwcli.UpdateOrders(ctx, ords)
			if err != nil {
				logger.Sugar().Errorw(
					"processBookKeepingGood",
					"Data", data,
					"Error", err,
				)
				return
			}
		}

		offset += limit
	}

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

	initialTriggerDone := make(chan struct{})

	go func() {
		for {
			select {
			case data := <-bookKeepingTrigger:
				logger.Sugar().Infow(
					"Watch",
					"State", "processBookKeepingGood manual start",
					"Data", data,
				)
				processBookKeepingGood(ctx, data)
				logger.Sugar().Infow(
					"Watch",
					"State", "processBookKeepingGood manual end",
					"Data", data,
				)
			case <-initialTriggerDone:
				return
			}
		}
	}()

	delay()
	close(initialTriggerDone)

	processWaitGoods(ctx, []string{})

	tickerWait := time.NewTicker(benefitInterval)
	tickerTransferring := time.NewTicker(checkInterval)
	checkChan := make(chan []string)

	for {
		select {
		case <-tickerWait.C:
			logger.Sugar().Infow(
				"Watch",
				"State", "processWaitGoods ticker start",
			)
			retryGoods := processWaitGoods(ctx, []string{})
			logger.Sugar().Infow(
				"Watch",
				"State", "processWaitGoods ticker end",
				"RetryGoods", retryGoods,
			)
			if len(retryGoods) > 0 {
				go func(retryGoods []string) {
					checkChan <- retryGoods
				}(retryGoods)
			}
		case _retryGoods, ok := <-checkChan:
			if !ok {
				continue
			}
			logger.Sugar().Infow(
				"Watch",
				"State", "processWaitGoods check start",
			)
			retryGoods := processWaitGoods(ctx, _retryGoods)
			logger.Sugar().Infow(
				"Watch",
				"State", "processWaitGoods checl end",
				"RetryGoods", retryGoods,
			)
			if len(retryGoods) > 0 {
				go func(retryGoods []string) {
					checkChan <- retryGoods
				}(retryGoods)
			}
		case <-tickerTransferring.C:
			processTransferringGoods(ctx)
			processBookKeepingGoods(ctx)
		case data := <-bookKeepingTrigger:
			logger.Sugar().Infow(
				"Watch",
				"State", "processBookKeepingGood manual start",
				"Data", data,
			)
			processBookKeepingGood(ctx, data)
			logger.Sugar().Infow(
				"Watch",
				"State", "processBookKeepingGood manual end",
				"Data", data,
			)
		case <-ctx.Done():
			logger.Sugar().Infow(
				"Watch",
				"State", "Done",
				"Error", ctx.Err(),
			)
			close(checkChan)
			return
		}
	}
}

func Redistribute(goodID, amount string, dateTime uint32) {
	go func() {
		logger.Sugar().Infow(
			"Redistribute",
			"GoodID", goodID,
			"Amount", amount,
			"DateTime", dateTime,
			"State", "Start",
		)
		bookKeepingTrigger <- &bookKeepingData{
			GoodID:   goodID,
			Amount:   amount,
			DateTime: dateTime,
		}
		logger.Sugar().Infow(
			"Redistribute",
			"GoodID", goodID,
			"Amount", amount,
			"DateTime", dateTime,
			"State", "End",
		)
	}()
}
