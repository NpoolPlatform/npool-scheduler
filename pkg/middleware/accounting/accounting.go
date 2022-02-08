package accounting

import (
	"context"
	"fmt"
	"time"

	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"
	goodspb "github.com/NpoolPlatform/message/npool/cloud-hashing-goods"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"
	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	usermgrpb "github.com/NpoolPlatform/message/npool/user"

	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	goodsconst "github.com/NpoolPlatform/cloud-hashing-goods/pkg/const"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	"github.com/google/uuid"

	"golang.org/x/xerrors"
)

const (
	secondsInDay  = uint32(24 * 60 * 60)
	secondsInHour = uint32(60 * 60) //nolint
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
	transferTicker  *time.Ticker
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
		if resp.Info == nil {
			logger.Sugar().Errorf("fail get platform setting by good [%v]", gac.good.ID)
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
			logger.Sugar().Errorf("fail get balance for good benefit account %v: %v [%v| %v %v]",
				gac.goodsetting.BenefitAccountID,
				err, gac.good.ID,
				gac.coininfo.Name,
				gac.accounts[gac.goodsetting.BenefitAccountID].Address)
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
				logger.Sugar().Errorf("fail get order user %v: %v", info.UserID, err)
				continue
			}

			// Only paid order should be involved
			respPayment, err := grpc2.GetPaymentByOrder(ctx, &orderpb.GetPaymentByOrderRequest{
				OrderID: info.ID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail to get payment of order %v", info.ID)
				continue
			}
			if respPayment.Info == nil {
				logger.Sugar().Errorf("order is not paid")
				continue
			}

			if respPayment.Info.State != orderconst.PaymentStateDone {
				logger.Sugar().Errorf("order %v not paid %+v", info.ID, respPayment.Info.ID)
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

func (ac *accounting) onCreateTransaction(ctx context.Context, gac *goodAccounting, totalAmount float64, benefitType string) error {
	toAddressID := gac.goodsetting.UserOnlineAccountID
	units := gac.userUnits

	if benefitType == "platform" {
		toAddressID = gac.goodsetting.PlatformOfflineAccountID
		units = gac.platformUnits
	}

	_, err := grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
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

	return nil
}

func (ac *accounting) onTransfer(ctx context.Context, transaction *billingpb.CoinAccountTransaction) error {
	logger.Sugar().Infof("try transfer %v amount %v state %v",
		transaction.ID,
		transaction.Amount,
		transaction.State)

	from, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: transaction.FromAddressID,
	})
	if err != nil {
		return xerrors.Errorf("fail get from address: %v [%v]", err, transaction.FromAddressID)
	}

	to, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: transaction.ToAddressID,
	})
	if err != nil {
		return xerrors.Errorf("fail get to address: %v [%v]", err, transaction.ToAddressID)
	}

	coininfo, err := grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
		ID: transaction.CoinTypeID,
	})
	if err != nil {
		return xerrors.Errorf("fail get coin info: %v", err)
	}

	// Transfer to chain
	logger.Sugar().Infof("transfer %v amount %v from %v to %v",
		transaction.ID,
		transaction.Amount,
		from.Info.Address,
		to.Info.Address)
	_, err = grpc2.CreateTransaction(ctx, &sphinxproxypb.CreateTransactionRequest{
		TransactionID: transaction.ID,
		Name:          coininfo.Info.Name,
		Amount:        transaction.Amount,
		From:          from.Info.Address,
		To:            to.Info.Address,
	})
	if err != nil {
		return xerrors.Errorf("fail create transaction: %v", err)
	}

	return nil
}

func (ac *accounting) onPersistentResult(ctx context.Context) { //nolint
	for _, gac := range ac.goodAccountings {
		if gac.good.BenefitType == goodsconst.BenefitTypePool {
			continue
		}

		_, err := grpc2.GetLatestPlatformBenefitByGood(ctx, &billingpb.GetLatestPlatformBenefitByGoodRequest{
			GoodID: gac.good.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get latest platform benefit by good: %v", err)
			continue
		}

		lastBenefitTimestamp := uint32(time.Now().Unix()) / secondsInDay * secondsInDay

		preQueryBalance := gac.preQueryBalance
		if preQueryBalance < gac.coininfo.ReservedAmount {
			preQueryBalance = gac.coininfo.ReservedAmount
		}

		totalAmount := gac.afterQueryBalanceInfo.Balance - preQueryBalance
		if totalAmount < 0 {
			logger.Sugar().Errorf("invalid amount: balance after query %v < before query %v (%v) [%v]",
				gac.afterQueryBalanceInfo.Balance,
				gac.preQueryBalance,
				preQueryBalance,
				gac.good.ID)
			continue
		}

		logger.Sugar().Infof("persistent result pre balance %v after balance %v reserved amount %v total amount %v",
			gac.preQueryBalance, gac.afterQueryBalanceInfo.Balance, gac.coininfo.ReservedAmount, totalAmount)

		_, err = grpc2.CreatePlatformBenefit(ctx, &billingpb.CreatePlatformBenefitRequest{
			Info: &billingpb.PlatformBenefit{
				GoodID:               gac.good.ID,
				BenefitAccountID:     gac.goodsetting.BenefitAccountID,
				Amount:               totalAmount,
				LastBenefitTimestamp: lastBenefitTimestamp,
				ChainTransactionID:   uuid.New().String(),
			},
		})
		if err != nil {
			logger.Sugar().Errorf("fail create platform benefit for good: %v [%v]", err, gac.good.ID)
			continue
		}

		if gac.userUnits > 0 {
			if err := ac.onCreateTransaction(ctx, gac, totalAmount, "user"); err != nil {
				logger.Sugar().Errorf("fail transfer: %v", err)
				continue
			}
			// TODO: check user online threshold and transfer to offline address
		}

		if gac.platformUnits > 0 {
			if err := ac.onCreateTransaction(ctx, gac, totalAmount, "platform"); err != nil {
				logger.Sugar().Errorf("fail transfer: %v", err)
				continue
			}
		}

		// Create user benefit according to valid order share of the good
		for _, order := range gac.orders {
			if gac.good.ID != order.GoodID {
				continue
			}

			_, err = grpc2.GetLatestUserBenefitByGoodAppUser(ctx, &billingpb.GetLatestUserBenefitByGoodAppUserRequest{
				GoodID: gac.good.ID,
				AppID:  order.AppID,
				UserID: order.UserID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail get latest user benefit by good: %v", err)
				continue
			}

			lastBenefitTimestamp := uint32(time.Now().Unix()) / secondsInDay * secondsInDay

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

func (ac *accounting) onCreatedChecker(ctx context.Context) {
	waitResp, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStateWait,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	payingResp, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStatePaying,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get paying transactions: %v", err)
		return
	}

	infos := []*billingpb.CoinAccountTransaction{}
	infos = append(infos, waitResp.Infos...)
	infos = append(infos, payingResp.Infos...)

	createdResp, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStateCreated,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	toWait := map[string]struct{}{}

	for _, created := range createdResp.Infos {
		logger.Sugar().Infof("try wait transaction %v amount %v state %v",
			created.ID,
			created.Amount,
			created.State)

		if _, ok := toWait[created.FromAddressID]; ok {
			continue
		}

		alreadyWaited := false
		for _, processing := range infos {
			if created.FromAddressID == processing.FromAddressID {
				alreadyWaited = true
				break
			}
		}

		if alreadyWaited {
			continue
		}

		logger.Sugar().Infof("transaction %v amount %v %v -> %v",
			created.ID,
			created.Amount,
			created.State,
			billingconst.CoinTransactionStateWait)
		created.State = billingconst.CoinTransactionStateWait
		_, err = grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
			Info: created,
		})
		if err != nil {
			logger.Sugar().Errorf("fail update transaction to wait: %v", err)
		}

		toWait[created.FromAddressID] = struct{}{}
	}
}

func (ac *accounting) onWaitChecker(ctx context.Context) {
	resp, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStateWait,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	for _, wait := range resp.Infos {
		if err := ac.onTransfer(ctx, wait); err != nil {
			logger.Sugar().Errorf("fail transfer transaction: %v", err)
			continue
		}

		logger.Sugar().Infof("transaction %v amount %v %v -> %v",
			wait.ID,
			wait.Amount,
			wait.State,
			billingconst.CoinTransactionStatePaying)

		wait.State = billingconst.CoinTransactionStatePaying
		_, err = grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
			Info: wait,
		})
		if err != nil {
			logger.Sugar().Errorf("fail update transaction to paying: %v", err)
		}
	}
}

func (ac *accounting) onPayingChecker(ctx context.Context) {
	resp, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStatePaying,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get paying transactions: %v", err)
		return
	}

	for _, paying := range resp.Infos {
		var toState string
		cid := paying.ChainTransactionID

		resp, err := grpc2.GetTransaction(ctx, &sphinxproxypb.GetTransactionRequest{
			TransactionID: paying.ID,
		})
		if err != nil {
			// If service not OK, do not update transaction state
			switch status.Code(err) {
			case codes.Unknown:
				logger.Sugar().Errorf("fail connect proxy service: %v", err)
				return
			case codes.InvalidArgument:
				toState = billingconst.CoinTransactionStateFail
			case codes.NotFound:
				toState = billingconst.CoinTransactionStateFail
			case codes.Internal:
				logger.Sugar().Errorf("fail get transaction state: %v", err)
				continue
			default:
				logger.Sugar().Errorf("grpc unexpected err: %v", err)
				continue
			}
		} else {
			switch resp.Info.TransactionState {
			case sphinxproxypb.TransactionState_TransactionStateFail:
				toState = billingconst.CoinTransactionStateFail
			case sphinxproxypb.TransactionState_TransactionStateDone:
				toState = billingconst.CoinTransactionStateSuccessful
				cid = resp.Info.CID
			// TODO: process review rejected
			// case sphinxproxypb.TransactionState_TransactionStateRejected:
			//toState = billingconst.CoinTransactionStateRejected
			default:
				continue
			}
		}

		// Update transaction according to the result of transaction stat
		logger.Sugar().Infof("transaction %v amount %v %v -> %v [%v]",
			paying.ID,
			paying.Amount,
			paying.State,
			toState,
			cid)

		paying.State = toState
		paying.ChainTransactionID = cid

		_, err = grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
			Info: paying,
		})
		if err != nil {
			logger.Sugar().Errorf("fail update transaction to %v: %v", toState, err)
		}
	}
}

func Run(ctx context.Context) {
	startAfter := (uint32(time.Now().Unix())/secondsInDay+1)*secondsInDay - secondsInHour*4
	startTimer := time.NewTimer(time.Duration(startAfter) * time.Second)
	<-startTimer.C

	ac := &accounting{
		scanTicker:     time.NewTicker(24 * time.Hour),
		transferTicker: time.NewTicker(30 * time.Second),
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
		case <-ac.transferTicker.C:
			ac.onCreatedChecker(ctx)
			ac.onWaitChecker(ctx)
			ac.onPayingChecker(ctx)
		}
	}
}
