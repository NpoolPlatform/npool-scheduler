package accounting

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"
	accountlock "github.com/NpoolPlatform/cloud-hashing-staker/pkg/middleware/account"
	currency "github.com/NpoolPlatform/cloud-hashing-staker/pkg/middleware/currency"

	appusermgrpb "github.com/NpoolPlatform/message/npool/appusermgr"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"
	goodspb "github.com/NpoolPlatform/message/npool/cloud-hashing-goods"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"
	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"

	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	goodsconst "github.com/NpoolPlatform/cloud-hashing-goods/pkg/const"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"golang.org/x/xerrors"
)

const (
	secondsInDay  = uint32(24 * 60 * 60)
	secondsInHour = uint32(60 * 60)
)

var benefitIntervalSeconds = secondsInDay

type goodAccounting struct {
	good                  *goodspb.GoodInfo
	coininfo              *coininfopb.CoinInfo
	coinsetting           *billingpb.CoinSetting
	goodbenefit           *billingpb.GoodBenefit
	preQueryBalance       float64
	afterQueryBalanceInfo *sphinxproxypb.BalanceInfo
	userUnits             uint32
	platformUnits         uint32
	accounts              map[string]*billingpb.CoinAccountInfo
	benefits              []*billingpb.PlatformBenefit
	transactions          []*billingpb.CoinAccountTransaction
	orders                []*orderpb.Order
	compensates           map[string][]*orderpb.Compensate
}

type accounting struct {
	scanTicker     *time.Ticker
	transferTicker *time.Ticker
	checkLimits    chan struct{}

	checkWaitTransactions  chan *goodAccounting
	checkFailTransactions  chan *goodAccounting
	queryCoinInfo          chan *goodAccounting
	queryAccount           chan *goodAccounting
	queryAccountInfo       chan *goodAccounting
	queryBenefits          chan *goodAccounting
	querySpendTransactions chan *goodAccounting
	queryBalance           chan *goodAccounting
	queryOrders            chan *goodAccounting
	queryCompensates       chan *goodAccounting
	caculateUserBenefit    chan *goodAccounting
	persistentResult       chan *goodAccounting
}

func (ac *accounting) onQueryGoods(ctx context.Context) {
	goods, err := grpc2.GetGoods(ctx, &goodspb.GetGoodsRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get goods: %v", err)
		return
	}

	for _, good := range goods {
		logger.Sugar().Infof("start accounting for good %v [%v]", good.ID, good.Title)
		go func(myGood *goodspb.GoodInfo) {
			ac.checkWaitTransactions <- &goodAccounting{
				good:        myGood,
				accounts:    map[string]*billingpb.CoinAccountInfo{},
				compensates: map[string][]*orderpb.Compensate{},
			}
		}(good)
	}
}

func (gac *goodAccounting) onCheckTransactionsByState(ctx context.Context, state string) error {
	txs, err := grpc2.GetCoinAccountTransactionsByGoodState(ctx, &billingpb.GetCoinAccountTransactionsByGoodStateRequest{
		GoodID: gac.good.ID,
		State:  state,
	})
	if err != nil {
		return xerrors.Errorf("fail get %v transactions: %v", state, err)
	}

	if len(txs) > 0 {
		return xerrors.Errorf("%v transactions not empty", state)
	}

	return nil
}

func (gac *goodAccounting) onCheckWaitTransactions(ctx context.Context) error {
	if err := gac.onCheckTransactionsByState(ctx, billingconst.CoinTransactionStateWait); err != nil {
		return xerrors.Errorf("fail check transactions: %v", err)
	}

	if err := gac.onCheckTransactionsByState(ctx, billingconst.CoinTransactionStateCreated); err != nil {
		return xerrors.Errorf("fail check transactions: %v", err)
	}

	if err := gac.onCheckTransactionsByState(ctx, billingconst.CoinTransactionStatePaying); err != nil {
		return xerrors.Errorf("fail check transactions: %v", err)
	}

	return nil
}

func (gac *goodAccounting) onCheckFailTransactions(ctx context.Context) error {
	txs, err := grpc2.GetCoinAccountTransactionsByGoodState(ctx, &billingpb.GetCoinAccountTransactionsByGoodStateRequest{
		GoodID: gac.good.ID,
		State:  billingconst.CoinTransactionStateFail,
	})
	if err != nil {
		return xerrors.Errorf("fail get fail transactions: %v", err)
	}

	for _, info := range txs {
		if info.FailHold {
			return xerrors.Errorf("fail hold by fail transaction")
		}
	}

	return nil
}

func (gac *goodAccounting) onQueryCoininfo(ctx context.Context) error {
	coinInfo, err := grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
		ID: gac.good.CoinInfoID,
	})
	if err != nil {
		return xerrors.Errorf("fail get coin info: %v [%v]", err, gac.good.ID)
	}

	if coinInfo.PreSale {
		return xerrors.Errorf("presale product cannot do accounting")
	}

	gac.coininfo = coinInfo

	coinSetting, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
		CoinTypeID: gac.good.CoinInfoID,
	})
	if err != nil || coinSetting == nil {
		return xerrors.Errorf("fail get coin setting: %v", err)
	}

	gac.coinsetting = coinSetting
	return nil
}

func (gac *goodAccounting) onQueryAccount(ctx context.Context) error {
	benefit, err := grpc2.GetGoodBenefitByGood(ctx, &billingpb.GetGoodBenefitByGoodRequest{
		GoodID: gac.good.ID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good benefit by good: %v [%v]", err, gac.good.ID)
	}
	if benefit == nil {
		return xerrors.Errorf("fail get good benefit by good [%v]", gac.good.ID)
	}

	gac.goodbenefit = benefit
	return nil
}

func (gac *goodAccounting) onQueryAccountInfo(ctx context.Context) error {
	account, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.goodbenefit.BenefitAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good benefit account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.goodbenefit.BenefitAccountID] = account

	account, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.coinsetting.PlatformOfflineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good platform offline account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.coinsetting.PlatformOfflineAccountID] = account

	account, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.coinsetting.UserOnlineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good user online benefit account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.coinsetting.UserOnlineAccountID] = account

	account, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.coinsetting.UserOfflineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good user offline benefit account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.coinsetting.UserOfflineAccountID] = account
	return nil
}

func (gac *goodAccounting) onQueryBenefits(ctx context.Context) error {
	benefits, err := grpc2.GetPlatformBenefitsByGood(ctx, &billingpb.GetPlatformBenefitsByGoodRequest{
		GoodID: gac.good.ID,
	})
	if err != nil {
		return xerrors.Errorf("fail get platform benefits by good: %v [%v]", err, gac.good.ID)
	}

	gac.benefits = benefits
	return nil
}

func (gac *goodAccounting) onQuerySpendTransactions(ctx context.Context) error {
	transactions, err := grpc2.GetCoinAccountTransactionsByCoinAccount(ctx, &billingpb.GetCoinAccountTransactionsByCoinAccountRequest{
		CoinTypeID: gac.good.CoinInfoID,
		AddressID:  gac.goodbenefit.BenefitAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get benefit account transaction by good: %v [%v]", err, gac.good.ID)
	}

	txs := []*billingpb.CoinAccountTransaction{}
	for _, info := range transactions {
		if info.ToAddressID == gac.goodbenefit.BenefitAccountID {
			logger.Sugar().Errorf("good benefit account should not accept platform incoming transaction: %v [%v]", info.ToAddressID, gac.good.ID)
			continue
		}
		txs = append(txs, info)
	}

	gac.transactions = txs
	return nil
}

func (gac *goodAccounting) onQueryBalance(ctx context.Context) error {
	inComing := float64(0)
	outComing := float64(0)

	for _, benefit := range gac.benefits {
		inComing += benefit.Amount
	}

	for _, spend := range gac.transactions {
		outComing += spend.Amount
	}

	if math.Abs(inComing-outComing) > 1 && inComing < outComing {
		return xerrors.Errorf("address %v invalid incoming %v < outcoming %v [%v]", gac.goodbenefit.BenefitAccountID, inComing, outComing, gac.good.ID)
	}

	account, ok := gac.accounts[gac.goodbenefit.BenefitAccountID]
	if !ok {
		return xerrors.Errorf("invalid benefit address")
	}

	balance, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    gac.coininfo.Name,
		Address: account.Address,
	})
	if err != nil || balance == nil {
		return xerrors.Errorf("fail get balance for good benefit account %v: %v [%v| %v %v]",
			gac.goodbenefit.BenefitAccountID,
			err, gac.good.ID,
			gac.coininfo.Name,
			account.Address)
	}

	gac.preQueryBalance = inComing - outComing
	gac.afterQueryBalanceInfo = balance
	return nil
}

func (gac *goodAccounting) onQueryOrders(ctx context.Context) error {
	orders, err := grpc2.GetOrdersByGood(ctx, &orderpb.GetOrdersByGoodRequest{
		GoodID: gac.good.ID,
	})
	if err != nil {
		return xerrors.Errorf("fail get orders by good: %v", err)
	}

	// TODO: multiple pages order
	validOrders := []*orderpb.Order{}

	for _, info := range orders {
		_, err := grpc2.GetAppUserByAppUser(ctx, &appusermgrpb.GetAppUserByAppUserRequest{
			AppID:  info.AppID,
			UserID: info.UserID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get order user %v: %v", info.UserID, err)
			continue
		}

		if uint32(time.Now().Unix()) < info.Start {
			continue
		}
		if info.End < uint32(time.Now().Unix()) {
			continue
		}

		// Only paid order should be involved
		payment, err := grpc2.GetPaymentByOrder(ctx, &orderpb.GetPaymentByOrderRequest{
			OrderID: info.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail to get payment of order %v", info.ID)
			continue
		}
		if payment == nil {
			logger.Sugar().Errorf("order %v is not paid", info.ID)
			continue
		}

		if payment.State != orderconst.PaymentStateDone {
			logger.Sugar().Errorf("order %v not paid %+v", info.ID, payment.ID)
			continue
		}

		validOrders = append(validOrders, info)
	}

	gac.orders = validOrders
	return nil
}

func (gac *goodAccounting) onQueryCompensates(ctx context.Context) {
	for _, order := range gac.orders {
		compensates, err := grpc2.GetCompensatesByOrder(ctx, &orderpb.GetCompensatesByOrderRequest{
			OrderID: order.ID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get compensates by order: %v", err)
			continue
		}

		gac.compensates[order.ID] = compensates
	}
}

func (gac *goodAccounting) onCaculateUserBenefit() {
	if gac.good.BenefitType == goodsconst.BenefitTypePool {
		return
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

	if uint32(gac.good.Total) > gac.userUnits {
		gac.platformUnits = uint32(gac.good.Total) - gac.userUnits
	}
}

func (gac *goodAccounting) onCreateBenefitTransaction(ctx context.Context, totalAmount float64, benefitType string) (string, error) {
	toAddressID := gac.coinsetting.UserOnlineAccountID
	units := gac.userUnits

	if benefitType == "platform" {
		toAddressID = gac.coinsetting.PlatformOfflineAccountID
		units = gac.platformUnits
	}

	amount := totalAmount * float64(units) * 1.0 / float64(gac.good.Total)
	amount = math.Floor(amount*10000) / 10000

	tx, err := grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
		Info: &billingpb.CoinAccountTransaction{
			AppID:              uuid.UUID{}.String(),
			UserID:             uuid.UUID{}.String(),
			GoodID:             gac.good.ID,
			FromAddressID:      gac.goodbenefit.BenefitAccountID,
			ToAddressID:        toAddressID,
			CoinTypeID:         gac.coininfo.ID,
			Amount:             amount,
			Message:            fmt.Sprintf("%v benefit of %v units %v total %v at %v", benefitType, gac.good.ID, units, gac.good.Total, time.Now()),
			ChainTransactionID: "",
		},
	})
	if err != nil {
		return "", xerrors.Errorf("fail create coin account transaction: %v", err)
	}

	return tx.ID, nil
}

func onCoinLimitsChecker(ctx context.Context, coinInfo *coininfopb.CoinInfo) error { //nolint
	warmCoinLimit := 100
	coinSetting, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
		CoinTypeID: coinInfo.ID,
	})
	if err != nil || coinSetting == nil {
		return xerrors.Errorf("fail get coin setting: %v", err)
	}

	platformSetting, err := grpc2.GetPlatformSetting(ctx, &billingpb.GetPlatformSettingRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get platform setting: %v", err)
	}

	if coinSetting != nil {
		warmCoinLimit = int(coinSetting.WarmAccountCoinAmount)
	} else if platformSetting != nil {
		price, err := currency.USDPrice(ctx, coinInfo.Name)
		if err == nil && price > 0 {
			warmCoinLimit = int(platformSetting.WarmAccountUSDAmount / price)
		}
	}

	account, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: coinSetting.UserOnlineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get user online benefit account id: %v", err)
	}

	_, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: coinSetting.UserOfflineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get user offline benefit account id: %v", err)
	}

	balance, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coinInfo.Name,
		Address: account.Address,
	})
	if err != nil {
		return xerrors.Errorf("fail get balance for account %v: %v [%v %v]",
			coinSetting.UserOnlineAccountID,
			err, coinInfo.Name,
			account.Address)
	}

	if int(balance.Balance) > warmCoinLimit && int(balance.Balance)-warmCoinLimit > warmCoinLimit {
		amount := balance.Balance - float64(warmCoinLimit)
		amount = math.Floor(amount*10000) / 10000

		_, err := grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
			Info: &billingpb.CoinAccountTransaction{
				AppID:              uuid.UUID{}.String(),
				UserID:             uuid.UUID{}.String(),
				GoodID:             uuid.UUID{}.String(),
				FromAddressID:      coinSetting.UserOnlineAccountID,
				ToAddressID:        coinSetting.UserOfflineAccountID,
				CoinTypeID:         coinInfo.ID,
				Amount:             amount,
				Message:            fmt.Sprintf("warm transfer at %v", time.Now()),
				ChainTransactionID: "",
			},
		})
		if err != nil {
			return xerrors.Errorf("fail create coin account transaction: %v", err)
		}
	}

	return nil
}

func onLimitsChecker(ctx context.Context) {
	coins, err := grpc2.GetCoinInfos(ctx, &coininfopb.GetCoinInfosRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get coin infos: %v", err)
		return
	}

	for _, info := range coins {
		err = onCoinLimitsChecker(ctx, info)
		if err != nil {
			logger.Sugar().Errorf("fail check coin limit: %v", err)
		}
	}
}

func onTransfer(ctx context.Context, transaction *billingpb.CoinAccountTransaction) error {
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
	logger.Sugar().Infof("transfer %v amount %v from %v to %v coin %v",
		transaction.ID,
		transaction.Amount,
		from.Address,
		to.Address,
		coininfo.Name)

	err = accountlock.Lock(from.ID)
	if err != nil {
		return xerrors.Errorf("fail lock account: %v", err)
	}

	_, err = grpc2.CreateTransaction(ctx, &sphinxproxypb.CreateTransactionRequest{
		TransactionID: transaction.ID,
		Name:          coininfo.Name,
		Amount:        transaction.Amount,
		From:          from.Address,
		To:            to.Address,
	})
	if err != nil {
		return xerrors.Errorf("fail create transaction: %v", err)
	}

	return nil
}

func (gac *goodAccounting) onPersistentResult(ctx context.Context) { //nolint
	if gac.good.BenefitType == goodsconst.BenefitTypePool {
		return
	}

	_, err := grpc2.GetLatestPlatformBenefitByGood(ctx, &billingpb.GetLatestPlatformBenefitByGoodRequest{
		GoodID: gac.good.ID,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get latest platform benefit by good: %v", err)
		return
	}

	lastBenefitTimestamp := uint32(time.Now().Unix()) / benefitIntervalSeconds * benefitIntervalSeconds

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
		return
	}

	logger.Sugar().Infof("persistent result pre balance %v after balance %v reserved amount %v total amount %v",
		gac.preQueryBalance, gac.afterQueryBalanceInfo.Balance, gac.coininfo.ReservedAmount, totalAmount)

	_, err = grpc2.CreatePlatformBenefit(ctx, &billingpb.CreatePlatformBenefitRequest{
		Info: &billingpb.PlatformBenefit{
			GoodID:               gac.good.ID,
			BenefitAccountID:     gac.goodbenefit.BenefitAccountID,
			Amount:               totalAmount,
			LastBenefitTimestamp: lastBenefitTimestamp,
			ChainTransactionID:   "",
		},
	})
	if err != nil {
		logger.Sugar().Errorf("fail create platform benefit for good: %v [%v]", err, gac.good.ID)
		return
	}

	var userTID string

	if gac.userUnits > 0 {
		id, err := gac.onCreateBenefitTransaction(ctx, totalAmount, "user")
		if err != nil {
			logger.Sugar().Errorf("fail transfer: %v", err)
			return
		}
		userTID = id
	}

	if gac.platformUnits > 0 {
		if _, err := gac.onCreateBenefitTransaction(ctx, totalAmount, "platform"); err != nil {
			logger.Sugar().Errorf("fail transfer: %v", err)
			return
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

		lastBenefitTimestamp := uint32(time.Now().Unix()) / benefitIntervalSeconds * benefitIntervalSeconds

		_, err = grpc2.CreateUserBenefit(ctx, &billingpb.CreateUserBenefitRequest{
			Info: &billingpb.UserBenefit{
				AppID:                 order.AppID,
				UserID:                order.UserID,
				GoodID:                order.GoodID,
				CoinTypeID:            gac.coininfo.ID,
				Amount:                totalAmount * float64(order.Units) * 1.0 / float64(gac.good.Total),
				LastBenefitTimestamp:  lastBenefitTimestamp,
				OrderID:               order.ID,
				PlatformTransactionID: userTID,
			},
		})
		if err != nil {
			logger.Sugar().Errorf("fail create user benefit: %v", err)
			continue
		}
	}
}

func onCreatedChecker(ctx context.Context) {
	waitTxs, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStateWait,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	payingTxs, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStatePaying,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get paying transactions: %v", err)
		return
	}

	infos := []*billingpb.CoinAccountTransaction{}
	infos = append(infos, waitTxs...)
	infos = append(infos, payingTxs...)

	createdTxs, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStateCreated,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	toWait := map[string]struct{}{}

	for _, created := range createdTxs {
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

func onWaitChecker(ctx context.Context) {
	txs, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStateWait,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	for _, wait := range txs {
		if err := onTransfer(ctx, wait); err != nil {
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

func onPayingChecker(ctx context.Context) { //nolint
	txs, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStatePaying,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get paying transactions: %v", err)
		return
	}

	for _, paying := range txs {
		var toState string
		cid := paying.ChainTransactionID

		tx, err := grpc2.GetTransaction(ctx, &sphinxproxypb.GetTransactionRequest{
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
			switch tx.TransactionState {
			case sphinxproxypb.TransactionState_TransactionStateFail:
				toState = billingconst.CoinTransactionStateFail
			case sphinxproxypb.TransactionState_TransactionStateDone:
				toState = billingconst.CoinTransactionStateSuccessful
				cid = tx.CID
			// TODO: process review rejected
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

		if toState == billingconst.CoinTransactionStateSuccessful && cid == "" {
			paying.Message = fmt.Sprintf("%v (successful without CID)", paying.Message)
			toState = billingconst.CoinTransactionStateFail
		}

		paying.State = toState
		paying.ChainTransactionID = cid
		if toState == sphinxproxypb.TransactionState_TransactionStateFail.String() {
			paying.FailHold = true
		}

		_, err = grpc2.UpdateCoinAccountTransaction(ctx, &billingpb.UpdateCoinAccountTransactionRequest{
			Info: paying,
		})
		if err != nil {
			logger.Sugar().Errorf("fail update transaction to %v: %v", toState, err)
			continue
		}

		err = accountlock.Unlock(paying.FromAddressID)
		if err != nil {
			logger.Sugar().Errorf("fail unlock account: %v", err)
		}
	}
}

func Run(ctx context.Context) { //nolint
	intervalStr := os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS")
	if intervalStr != "" {
		seconds, err := strconv.ParseUint(intervalStr, 10, 64)
		if err == nil {
			benefitIntervalSeconds = uint32(seconds)
		}
	}

	logger.Sugar().Infof("caculate benefit each %v seconds", benefitIntervalSeconds)

	now := uint32(time.Now().Unix())
	startAfter := (now/secondsInDay+1)*secondsInDay - secondsInHour*4 - now
	if startAfter > benefitIntervalSeconds {
		startAfter = benefitIntervalSeconds
	}
	startTimer := time.NewTimer(time.Duration(startAfter) * time.Second)
	logger.Sugar().Infof("wait for %v seconds", startAfter)
	<-startTimer.C

	ac := &accounting{
		scanTicker:     time.NewTicker(time.Duration(benefitIntervalSeconds) * time.Second),
		transferTicker: time.NewTicker(30 * time.Second),
		checkLimits:    make(chan struct{}),

		checkWaitTransactions:  make(chan *goodAccounting),
		checkFailTransactions:  make(chan *goodAccounting),
		queryCoinInfo:          make(chan *goodAccounting),
		queryAccount:           make(chan *goodAccounting),
		queryAccountInfo:       make(chan *goodAccounting),
		queryBenefits:          make(chan *goodAccounting),
		querySpendTransactions: make(chan *goodAccounting),
		queryBalance:           make(chan *goodAccounting),
		queryOrders:            make(chan *goodAccounting),
		queryCompensates:       make(chan *goodAccounting),
		caculateUserBenefit:    make(chan *goodAccounting),
		persistentResult:       make(chan *goodAccounting),
	}

	for {
		select {
		case <-ac.scanTicker.C:
			logger.Sugar().Infof("start query goods")
			ac.onQueryGoods(ctx)

		case gac := <-ac.checkWaitTransactions:
			if err := gac.onCheckWaitTransactions(ctx); err != nil {
				logger.Sugar().Errorf("fail check wait transaction: %v", err)
				continue
			}
			go func() { ac.checkFailTransactions <- gac }()

		case gac := <-ac.checkFailTransactions:
			if err := gac.onCheckFailTransactions(ctx); err != nil {
				logger.Sugar().Errorf("fail check fail transaction: %v", err)
				continue
			}
			go func() { ac.queryCoinInfo <- gac }()

		case gac := <-ac.queryCoinInfo:
			if err := gac.onQueryCoininfo(ctx); err != nil {
				logger.Sugar().Errorf("fail query coin info: %v", err)
				continue
			}
			go func() { ac.queryAccount <- gac }()

		case gac := <-ac.queryAccount:
			if err := gac.onQueryAccount(ctx); err != nil {
				logger.Sugar().Errorf("fail query account: %v", err)
				continue
			}
			go func() { ac.queryAccountInfo <- gac }()

		case gac := <-ac.queryAccountInfo:
			if err := gac.onQueryAccountInfo(ctx); err != nil {
				logger.Sugar().Errorf("fail query account info: %v", err)
				continue
			}
			go func() { ac.queryBenefits <- gac }()

		case gac := <-ac.queryBenefits:
			if err := gac.onQueryBenefits(ctx); err != nil {
				logger.Sugar().Errorf("fail query benefits: %v", err)
				continue
			}
			go func() { ac.querySpendTransactions <- gac }()

		case gac := <-ac.querySpendTransactions:
			if err := gac.onQuerySpendTransactions(ctx); err != nil {
				logger.Sugar().Errorf("fail query spend transactions: %v", err)
				continue
			}
			go func() { ac.queryBalance <- gac }()

		case gac := <-ac.queryBalance:
			if err := gac.onQueryBalance(ctx); err != nil {
				logger.Sugar().Errorf("fail query balance: %v", err)
				continue
			}
			go func() { ac.queryOrders <- gac }()

		case gac := <-ac.queryOrders:
			if err := gac.onQueryOrders(ctx); err != nil {
				logger.Sugar().Errorf("fail query orders: %v", err)
				continue
			}
			go func() { ac.queryCompensates <- gac }()

		case gac := <-ac.queryCompensates:
			gac.onQueryCompensates(ctx)
			go func() { ac.caculateUserBenefit <- gac }()

		case gac := <-ac.caculateUserBenefit:
			gac.onCaculateUserBenefit()
			go func() { ac.persistentResult <- gac }()

		case gac := <-ac.persistentResult:
			gac.onPersistentResult(ctx)

		case <-ac.transferTicker.C:
			onCreatedChecker(ctx)
			onWaitChecker(ctx)
			onPayingChecker(ctx)
			go func() { ac.checkLimits <- struct{}{} }()

		case <-ac.checkLimits:
			onLimitsChecker(ctx)
		}
	}
}
