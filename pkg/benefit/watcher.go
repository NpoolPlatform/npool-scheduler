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
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"

	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"

	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordercli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	accountmgrpb "github.com/NpoolPlatform/message/npool/account/mgr/v1/account"
	paymentmgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/payment"

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

func validateGoodOrder(good *goodmwpb.Good, order *orderpb.Order, waiting bool) bool {
	if order.PaymentState != paymentmgrpb.PaymentState_Done {
		return false
	}

	orderEnd := order.CreatedAt + uint32(good.DurationDays*secondsPerDay)
	if orderEnd < uint32(time.Now().Unix()) {
		return false
	}
	if !waiting {
		if order.Start > uint32(time.Now().Unix()) {
			return false
		}
	}

	return true
}

func processGood(ctx context.Context, good *goodmwpb.Good) error { //nolint
	if good.StartAt > uint32(time.Now().Unix()) {
		return nil
	}

	coin, err := coinmwcli.GetCoin(ctx, good.CoinTypeID)
	if err != nil {
		return fmt.Errorf("good coin error: %v", err)
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	if coin.Presale {
		return nil
	}

	userOnline, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: good.CoinTypeID,
		},
		UsedFor: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(accountmgrpb.AccountUsedFor_UserBenefitHot),
		},
		Backup: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
	})
	if err != nil {
		return fmt.Errorf("user online account error: %v", err)
	}
	if userOnline == nil {
		return fmt.Errorf("invalid hot account")
	}

	pltfOffline, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: good.CoinTypeID,
		},
		UsedFor: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(accountmgrpb.AccountUsedFor_PlatformBenefitCold),
		},
		Backup: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
	})
	if err != nil {
		return fmt.Errorf("platform offline account error: %v", err)
	}
	if pltfOffline == nil {
		return fmt.Errorf("invalid cold account")
	}

	_gp := &gp{
		goodID:                   good.ID,
		goodName:                 good.Title,
		benefitIntervalHours:     good.BenefitIntervalHours,
		coinTypeID:               coin.ID,
		coinName:                 coin.Name,
		coinReservedAmount:       decimal.RequireFromString(coin.ReservedAmount),
		userOnlineAccountID:      userOnline.AccountID,
		platformOfflineAccountID: pltfOffline.AccountID,
	}

	if err := _gp.processDailyProfit(ctx); err != nil {
		return fmt.Errorf("process daily profit error: %v", err)
	}

	logger.Sugar().Infow("benefit", "goodID", good.ID, "goodName", good.Title, "profit", _gp.dailyProfit)

	if _gp.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		logger.Sugar().Infow("benefit", "goodID", good.ID, "goodName", good.Title, "dailyProfit", _gp.dailyProfit)
		return nil
	}

	if err := _gp.stock(ctx); err != nil {
		return fmt.Errorf("process good stock error: %v", err)
	}

	offset := int32(0)
	limit := int32(1000)

	for {
		// TODO: GetOrders with state
		orders, _, err := ordercli.GetOrders(ctx, &orderpb.Conds{
			GoodID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: good.ID,
			},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}

		for _, order := range orders {
			validate := validateGoodOrder(good, order, true)
			if !validate {
				continue
			}
			_gp.totalOrderUnits += order.Units
		}

		offset += limit
	}

	if _gp.totalUnits < _gp.totalOrderUnits || _gp.inService != _gp.totalOrderUnits {
		return fmt.Errorf("invalid units total %v, orderUnits %v, inService %v", _gp.totalUnits, _gp.totalOrderUnits, _gp.inService)
	}

	offset = 0

	for {
		// TODO: GetOrders with state
		orders, _, err := ordercli.GetOrders(ctx, &orderpb.Conds{
			GoodID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: good.ID,
			},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}

		for _, order := range orders {
			validate := validateGoodOrder(good, order, false)
			if !validate {
				continue
			}

			_gp.serviceUnits++

			if err := _gp.processOrder(ctx, order); err != nil {
				return fmt.Errorf("process order error: %v", err)
			}
		}

		offset += limit
	}

	if err := _gp.transfer(ctx); err != nil {
		return fmt.Errorf("transfer error: %v", err)
	}

	if err := _gp.addDailyProfit(ctx); err != nil {
		return fmt.Errorf("add daily profit error: %v", err)
	}

	if err := _gp.processUnsold(ctx); err != nil {
		return fmt.Errorf("process unsold error: %v", err)
	}

	return nil
}

func processWaitGoods(ctx context.Context) {
	offset := int32(0)
	limit := int32(100)
	state := &State{}

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
