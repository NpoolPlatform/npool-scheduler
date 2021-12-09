package accounting

import (
	"context"
	"fmt"
	"time"

	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"

	billingpb "github.com/NpoolPlatform/cloud-hashing-billing/message/npool"
	goodspb "github.com/NpoolPlatform/cloud-hashing-goods/message/npool"
	orderpb "github.com/NpoolPlatform/cloud-hashing-order/message/npool"
	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxservicepb "github.com/NpoolPlatform/message/npool/sphinxservice"
	usermgrpb "github.com/NpoolPlatform/user-management/message/npool"

	goodsconst "github.com/NpoolPlatform/cloud-hashing-goods/pkg/const"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	"github.com/google/uuid"

	"golang.org/x/xerrors"
)

type goodAccounting struct {
	good                  *goodspb.GoodInfo
	coininfo              *coininfopb.CoinInfo
	goodsetting           *billingpb.PlatformSetting
	accounts              map[string]*billingpb.CoinAccountInfo
	benefits              []*billingpb.PlatformBenefit
	transactions          []*billingpb.CoinAccountTransaction
	preQueryBalance       float64
	afterQueryBalanceInfo *sphinxproxypb.BalanceInfo
	orders                []*orderpb.Order
	compensates           map[string][]*orderpb.Compensate
	userUnits             uint32
	platformUnits         uint32
}

type accounting struct {
	scanTicker      *time.Ticker
	waitTicker      *time.Ticker
	payingTicker    *time.Ticker
	goodAccountings []*goodAccounting
}

func (ac *accounting) onQueryGoods(ctx context.Context) {
	resp, err := grpc2.GetGoods(ctx, &goodspb.GetGoodsRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get goods: %v", err)
		return
	}

	acs := []*goodAccounting{}
	for _, good := range resp.Infos {
		acs = append(acs, &goodAccounting{
			good:        good,
			accounts:    map[string]*billingpb.CoinAccountInfo{},
			compensates: map[string][]*orderpb.Compensate{},
		})
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryCoininfo(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
			ID: gac.good.CoinInfoID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get coin info: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.coininfo = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryAccount(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetPlatformSettingByGood(ctx, &billingpb.GetPlatformSettingByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get platform setting by good: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.goodsetting = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryAccountInfo(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.BenefitAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good benefit account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.BenefitAccountID] = resp.Info

		resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.PlatformOfflineAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good platform offline account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.PlatformOfflineAccountID] = resp.Info

		resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.UserOnlineAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good user online benefit account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.UserOnlineAccountID] = resp.Info

		resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: gac.goodsetting.UserOfflineAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get good user offline benefit account id: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.accounts[gac.goodsetting.UserOfflineAccountID] = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryBenefits(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetPlatformBenefitsByGood(ctx, &billingpb.GetPlatformBenefitsByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get platform benefits by good: %v [%v]", err, gac.good.ID)
			continue
		}

		gac.benefits = resp.Infos
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQuerySpendTransactions(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetCoinAccountTransactionsByCoinAccount(ctx, &billingpb.GetCoinAccountTransactionsByCoinAccountRequest{
			CoinTypeID: gac.good.CoinInfoID,
			AddressID:  gac.goodsetting.BenefitAccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get benefit account transaction by good: %v [%v]", err, gac.good.ID)
			continue
		}

		for _, info := range resp.Infos {
			if info.ToAddressID == gac.goodsetting.BenefitAccountID {
				logger.Sugar().Errorf("good benefit account should not accept platform incoming transaction: %v [%v]", info.ToAddressID, gac.good.ID)
				continue
			}
		}

		gac.transactions = resp.Infos
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryBalance(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		inComing := float64(0)
		outComing := float64(0)

		for _, benefit := range gac.benefits {
			inComing += benefit.Amount
		}

		for _, spend := range gac.transactions {
			outComing += spend.Amount
		}

		if inComing < outComing {
			logger.Sugar().Errorf("address %v invalid incoming %v < outcoming %v [%v]", gac.goodsetting.BenefitAccountID, inComing, outComing, gac.good.ID)
			continue
		}

		resp, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    gac.coininfo.Name,
			Address: gac.accounts[gac.goodsetting.BenefitAccountID].Address,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get balance for good benefit account %v: %v [%v]", gac.goodsetting.BenefitAccountID, err, gac.good.ID)
			continue
		}

		gac.preQueryBalance = inComing - outComing
		gac.afterQueryBalanceInfo = resp.Info
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryOrders(ctx context.Context) {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		resp, err := grpc2.GetOrdersByGood(ctx, &orderpb.GetOrdersByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get orders by good: %v", err)
			continue
		}

		orders := []*orderpb.Order{}
		for _, info := range resp.Infos {
			_, err := grpc2.GetUser(ctx, &usermgrpb.GetUserRequest{
				AppID:  info.AppID,
				UserID: info.UserID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail get order user: %v", err)
				continue
			}
			orders = append(orders, info)
		}

		gac.orders = orders
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onQueryCompensates(ctx context.Context) {
	for _, gac := range ac.goodAccountings {
		for _, order := range gac.orders {
			resp, err := grpc2.GetCompensatesByOrder(ctx, &orderpb.GetCompensatesByOrderRequest{
				OrderID: order.ID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail get compensates by order: %v", err)
				continue
			}

			gac.compensates[order.ID] = resp.Infos
		}
	}
}

func (ac *accounting) onCaculateUserBenefit() {
	acs := []*goodAccounting{}

	for _, gac := range ac.goodAccountings {
		if gac.good.BenefitType == goodsconst.BenefitTypePool {
			continue
		}

		gac.userUnits = 0
		gac.platformUnits = 0
		goodDurationSeconds := uint32(gac.good.DurationDays * 24 * 60 * 60)
		nowSeconds := uint32(time.Now().Unix())

		for _, order := range gac.orders {
			compensateSeconds := uint32(0)
			for _, compensate := range gac.compensates[order.ID] {
				compensateSeconds += compensate.End - compensate.Start
			}

			if order.Start+goodDurationSeconds+compensateSeconds < nowSeconds {
				continue
			}

			gac.userUnits += order.Units
		}

		gac.platformUnits = uint32(gac.good.Total) - gac.userUnits
		acs = append(acs, gac)
	}
	ac.goodAccountings = acs
}

func (ac *accounting) onTransfer(ctx context.Context, gac *goodAccounting, totalAmount float64, benefitType string) error {
	toAddressID := gac.goodsetting.UserOnlineAccountID
	units := gac.userUnits

	if benefitType == "platform" {
		toAddressID = gac.goodsetting.PlatformOfflineAccountID
		units = gac.platformUnits
	}

	resp, err := grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
		Info: &billingpb.CoinAccountTransaction{
			AppID:                 uuid.UUID{}.String(),
			UserID:                uuid.UUID{}.String(),
			FromAddressID:         gac.goodsetting.BenefitAccountID,
			ToAddressID:           toAddressID,
			CoinTypeID:            gac.coininfo.ID,
			Amount:                totalAmount * float64(units) * 1.0 / float64(gac.good.Total),
			Message:               fmt.Sprintf("%v benefit of %v at %v", benefitType, gac.good.ID, time.Now()),
			PlatformTransactionID: uuid.New().String(),
			ChainTransactionID:    uuid.New().String(),
		},
	})
	if err != nil {
		return xerrors.Errorf("fail create coin account transaction: %v", err)
	}

	// Transfer to chain
	logger.Sugar().Infof("transfer %v from %v to %v for %v",
		totalAmount*float64(units)*1.0/float64(gac.good.Total),
		gac.accounts[gac.goodsetting.BenefitAccountID].Address,
		gac.accounts[toAddressID].Address,
		benefitType)
	_, err = grpc2.CreateTransaction(ctx, &sphinxservicepb.CreateTransactionRequest{
		TransactionID: resp.Info.ID,
		Name:          gac.coininfo.Name,
		Amount:        totalAmount * float64(units) * 1.0 / float64(gac.good.Total),
		From:          gac.accounts[gac.goodsetting.BenefitAccountID].Address,
		To:            gac.accounts[toAddressID].Address,
	})
	if err != nil {
		err1 := xerrors.Errorf("fail create transaction: %v", err)
		resp.Info.State = "fail"
		_, err := grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
			Info: resp.Info,
		})
		if err != nil {
			return xerrors.Errorf("fail update transaction to fail: %v: %v", err1, err)
		}
		return xerrors.Errorf("fail update transaction to fail: %v", err1)
	}

	// Update coin account transaction state
	resp.Info.State = "paying"
	_, err = grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
		Info: resp.Info,
	})
	if err != nil {
		return xerrors.Errorf("fail update transaction to paying: %v", err)
	}

	return nil
}

func (ac *accounting) onPersistentResult(ctx context.Context) { //nolint
	for _, gac := range ac.goodAccountings {
		if gac.good.BenefitType == goodsconst.BenefitTypePool {
			continue
		}

		resp, err := grpc2.GetLatestPlatformBenefitByGood(ctx, &billingpb.GetLatestPlatformBenefitByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get latest platform benefit by good: %v", err)
			continue
		}

		secondsInDay := uint32(24 * 60 * 60)
		lastBenefitTimestamp := uint32(time.Now().Unix()) / secondsInDay * secondsInDay
		if resp.Info != nil {
			lastBenefitTimestamp = resp.Info.CreateAt / secondsInDay * secondsInDay
		}

		_, err = grpc2.CreatePlatformBenefit(ctx, &billingpb.CreatePlatformBenefitRequest{
			Info: &billingpb.PlatformBenefit{
				GoodID:               gac.good.ID,
				BenefitAccountID:     gac.goodsetting.BenefitAccountID,
				Amount:               gac.afterQueryBalanceInfo.Balance - gac.preQueryBalance,
				LastBenefitTimestamp: lastBenefitTimestamp,
				ChainTransactionID:   uuid.New().String(),
			},
		})
		if err != nil {
			logger.Sugar().Errorf("fail create platform benefit for good: %v", err)
			continue
		}

		totalAmount := gac.afterQueryBalanceInfo.Balance - gac.preQueryBalance
		if totalAmount < 0 {
			logger.Sugar().Errorf("invalid amount: balance after query %v < before query %v [%v]",
				gac.afterQueryBalanceInfo.Balance,
				gac.preQueryBalance,
				gac.good.ID)
			continue
		}

		if gac.userUnits > 0 {
			if err := ac.onTransfer(ctx, gac, totalAmount, "user"); err != nil {
				logger.Sugar().Errorf("fail transfer: %v", err)
				continue
			}
			// TODO: check user online threshold and transfer to offline address
		}

		if gac.platformUnits > 0 {
			if err := ac.onTransfer(ctx, gac, totalAmount, "platform"); err != nil {
				logger.Sugar().Errorf("fail transfer: %v", err)
				continue
			}
		}

		// TODO: create user benefit according to valid order share of the good
		for _, order := range gac.orders {
			if gac.good.ID != order.GoodID {
				continue
			}

			resp, err := grpc2.GetLatestUserBenefitByGoodAppUser(ctx, &billingpb.GetLatestUserBenefitByGoodAppUserRequest{
				GoodID: gac.good.ID,
				AppID:  order.AppID,
				UserID: order.UserID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail get latest user benefit by good: %v", err)
				continue
			}

			secondsInDay := uint32(24 * 60 * 60)
			lastBenefitTimestamp := uint32(time.Now().Unix()) / secondsInDay * secondsInDay
			if resp.Info != nil {
				lastBenefitTimestamp = resp.Info.CreateAt / secondsInDay * secondsInDay
			}

			_, err = grpc2.CreateUserBenefit(ctx, &billingpb.CreateUserBenefitRequest{
				Info: &billingpb.UserBenefit{
					UserID:               order.UserID,
					AppID:                order.AppID,
					GoodID:               order.GoodID,
					Amount:               totalAmount * float64(order.Units) * 1.0 / float64(gac.good.Total),
					LastBenefitTimestamp: lastBenefitTimestamp,
					OrderID:              order.ID,
				},
			})
			if err != nil {
				logger.Sugar().Errorf("fail create user benefit: %v", err)
				continue
			}
		}
	}
}

func (ac *accounting) onChecker(ctx context.Context, myState, failState, nextState string) {
	resp, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: myState,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	for _, transaction := range resp.Infos {
		_, err := grpc2.GetTransaction(ctx, &sphinxservicepb.GetTransactionRequest{
			TransactionID: transaction.ID,
		})
		// TODO: if service not OK, do not update transaction state
		if err != nil {
			logger.Sugar().Errorf("fail get transaction state: %v", err)

			transaction.State = failState
			_, err := grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
				Info: transaction,
			})
			if err != nil {
				logger.Sugar().Errorf("fail update transaction to %v: %v", err, failState)
			}
			continue
		}

		// TODO: update transaction according to the result of transaction stat

		transaction.State = nextState
		_, err = grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
			Info: transaction,
		})
		if err != nil {
			logger.Sugar().Errorf("fail update transaction to %v: %v", err, nextState)
		}
	}
}

func (ac *accounting) onWaitChecker(ctx context.Context) {
	ac.onChecker(ctx, "wait", "fail", "paying")
}

func (ac *accounting) onPayingChecker(ctx context.Context) {
	ac.onChecker(ctx, "paying", "fail", "successful")
}

func Run(ctx context.Context) {
	// TODO: when to start

	ac := &accounting{
		scanTicker:   time.NewTicker(24 * 60 * 60 * time.Second),
		waitTicker:   time.NewTicker(30 * time.Second),
		payingTicker: time.NewTicker(30 * time.Second),
	}

	for {
		select {
		case <-ac.scanTicker.C:
			ac.onQueryGoods(ctx)
			ac.onQueryCoininfo(ctx)
			ac.onQueryAccount(ctx)
			ac.onQueryAccountInfo(ctx)
			ac.onQueryBenefits(ctx)
			ac.onQuerySpendTransactions(ctx)
			ac.onQueryBalance(ctx)
			ac.onQueryOrders(ctx)
			ac.onQueryCompensates(ctx)
			ac.onCaculateUserBenefit()
			ac.onPersistentResult(ctx)
		case <-ac.waitTicker.C:
			ac.onWaitChecker(ctx)
		case <-ac.payingTicker.C:
			ac.onPayingChecker(ctx)
		}
	}
}
