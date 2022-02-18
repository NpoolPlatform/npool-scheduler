package accounting

import (
	"context"
	"fmt"
	"time"

	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"
	currency "github.com/NpoolPlatform/cloud-hashing-staker/pkg/middleware/currency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	"golang.org/x/xerrors"
)

const (
	secondsInDay  = uint32(24 * 60 * 60)
	secondsInHour = uint32(60 * 60) //nolint
)

type goodAccounting struct {
	good                  *goodspb.GoodInfo
	coininfo              *coininfopb.CoinInfo
	coinsetting           *billingpb.CoinSetting
	goodbenefit           *billingpb.GoodBenefit
	preQueryBalance       float64
	afterQueryBalanceInfo *sphinxproxypb.BalanceInfo
	userUnits             uint32
	platformUnits         uint32
	platformsetting       *billingpb.PlatformSetting
	accounts              map[string]*billingpb.CoinAccountInfo
	benefits              []*billingpb.PlatformBenefit
	transactions          []*billingpb.CoinAccountTransaction
	orders                []*orderpb.Order
	compensates           map[string][]*orderpb.Compensate
}

type accounting struct {
	scanTicker     *time.Ticker
	transferTicker *time.Ticker

	queryCoinInfo          chan *goodAccounting
	queryAccount           chan *goodAccounting
	queryAccountInfo       chan *goodAccounting
	queryBenefits          chan *goodAccounting
	querySpendTransactions chan *goodAccounting
	queryBalance           chan *goodAccounting
	queryOrders            chan *goodAccounting
	queryCompensates       chan *goodAccounting
	caculateUserBenefit    chan *goodAccounting
	checkLimits            chan *goodAccounting
	persistentResult       chan *goodAccounting
}

func (ac *accounting) onQueryGoods(ctx context.Context) {
	resp, err := grpc2.GetGoods(ctx, &goodspb.GetGoodsRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get goods: %v", err)
		return
	}

	resp1, err := grpc2.GetPlatformSetting(ctx, &billingpb.GetPlatformSettingRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get platform setting: %v", err)
		return
	}

	for _, good := range resp.Infos {
		logger.Sugar().Infof("start accounting for good %v [%v]", good.ID, good.Title)
		go func(myGood *goodspb.GoodInfo) {
			ac.queryCoinInfo <- &goodAccounting{
				good:            myGood,
				accounts:        map[string]*billingpb.CoinAccountInfo{},
				compensates:     map[string][]*orderpb.Compensate{},
				platformsetting: resp1.Info,
			}
		}(good)
	}
}

func (gac *goodAccounting) onQueryCoininfo(ctx context.Context) error {
	resp, err := grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
		ID: gac.good.CoinInfoID,
	})
	if err != nil {
		return xerrors.Errorf("fail get coin info: %v [%v]", err, gac.good.ID)
	}

	if resp.Info.PreSale {
		return xerrors.Errorf("presale product cannot do accounting")
	}

	gac.coininfo = resp.Info

	resp1, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
		CoinTypeID: gac.good.CoinInfoID,
	})
	if err != nil {
		return xerrors.Errorf("fail get coin setting: %v", err)
	}
	if resp1.Info == nil {
		return xerrors.Errorf("fail get coin setting")
	}

	gac.coinsetting = resp1.Info
	return nil
}

func (gac *goodAccounting) onQueryAccount(ctx context.Context) error {
	resp, err := grpc2.GetGoodBenefitByGood(ctx, &billingpb.GetGoodBenefitByGoodRequest{
		GoodID: gac.good.ID,
	})
	if err != nil {
		return xerrors.Errorf("fail get platform setting by good: %v [%v]", err, gac.good.ID)
	}
	if resp.Info == nil {
		return xerrors.Errorf("fail get platform setting by good [%v]", gac.good.ID)
	}

	gac.goodbenefit = resp.Info
	return nil
}

func (gac *goodAccounting) onQueryAccountInfo(ctx context.Context) error {
	resp, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.goodbenefit.BenefitAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good benefit account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.goodbenefit.BenefitAccountID] = resp.Info

	resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.coinsetting.PlatformOfflineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good platform offline account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.coinsetting.PlatformOfflineAccountID] = resp.Info

	resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.coinsetting.UserOnlineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good user online benefit account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.coinsetting.UserOnlineAccountID] = resp.Info

	resp, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: gac.coinsetting.UserOfflineAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get good user offline benefit account id: %v [%v]", err, gac.good.ID)
	}

	gac.accounts[gac.coinsetting.UserOfflineAccountID] = resp.Info
	return nil
}

func (gac *goodAccounting) onQueryBenefits(ctx context.Context) error {
	resp, err := grpc2.GetPlatformBenefitsByGood(ctx, &billingpb.GetPlatformBenefitsByGoodRequest{
		GoodID: gac.good.ID,
	})
	if err != nil {
		return xerrors.Errorf("fail get platform benefits by good: %v [%v]", err, gac.good.ID)
	}

	gac.benefits = resp.Infos
	return nil
}

func (gac *goodAccounting) onQuerySpendTransactions(ctx context.Context) error {
	resp, err := grpc2.GetCoinAccountTransactionsByCoinAccount(ctx, &billingpb.GetCoinAccountTransactionsByCoinAccountRequest{
		CoinTypeID: gac.good.CoinInfoID,
		AddressID:  gac.goodbenefit.BenefitAccountID,
	})
	if err != nil {
		return xerrors.Errorf("fail get benefit account transaction by good: %v [%v]", err, gac.good.ID)
	}

	txs := []*billingpb.CoinAccountTransaction{}
	for _, info := range resp.Infos {
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

	if inComing < outComing {
		return xerrors.Errorf("address %v invalid incoming %v < outcoming %v [%v]", gac.goodbenefit.BenefitAccountID, inComing, outComing, gac.good.ID)
	}

	account, ok := gac.accounts[gac.goodbenefit.BenefitAccountID]
	if !ok {
		return xerrors.Errorf("invalid benefit address")
	}

	resp, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    gac.coininfo.Name,
		Address: account.Address,
	})
	if err != nil {
		return xerrors.Errorf("fail get balance for good benefit account %v: %v [%v| %v %v]",
			gac.goodbenefit.BenefitAccountID,
			err, gac.good.ID,
			gac.coininfo.Name,
			account.Address)
	}
	if resp.Info == nil {
		return xerrors.Errorf("fail get balance for good benefit account %v: [%v| %v %v]",
			gac.goodbenefit.BenefitAccountID,
			gac.good.ID,
			gac.coininfo.Name,
			account.Address)
	}

	gac.preQueryBalance = inComing - outComing
	gac.afterQueryBalanceInfo = resp.Info
	return nil
}

func (gac *goodAccounting) onQueryOrders(ctx context.Context) error {
	resp, err := grpc2.GetOrdersByGood(ctx, &orderpb.GetOrdersByGoodRequest{
		GoodID: gac.good.ID,
	})
	if err != nil {
		return xerrors.Errorf("fail get orders by good: %v", err)
	}

	// TODO: multiple pages order
	orders := []*orderpb.Order{}
	for _, info := range resp.Infos {
		_, err := grpc2.GetAppUserByAppUser(ctx, &appusermgrpb.GetAppUserByAppUserRequest{
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
	return nil
}

func (gac *goodAccounting) onQueryCompensates(ctx context.Context) {
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

	gac.platformUnits = uint32(gac.good.Total) - gac.userUnits
}

func (gac *goodAccounting) onCreateBenefitTransaction(ctx context.Context, totalAmount float64, benefitType string) error {
	toAddressID := gac.coinsetting.UserOnlineAccountID
	units := gac.userUnits

	if benefitType == "platform" {
		toAddressID = gac.coinsetting.PlatformOfflineAccountID
		units = gac.platformUnits
	}

	_, err := grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
		Info: &billingpb.CoinAccountTransaction{
			AppID:              uuid.UUID{}.String(),
			UserID:             uuid.UUID{}.String(),
			FromAddressID:      gac.goodbenefit.BenefitAccountID,
			ToAddressID:        toAddressID,
			CoinTypeID:         gac.coininfo.ID,
			Amount:             totalAmount * float64(units) * 1.0 / float64(gac.good.Total),
			Message:            fmt.Sprintf("%v benefit of %v at %v", benefitType, gac.good.ID, time.Now()),
			ChainTransactionID: uuid.New().String(),
		},
	})
	if err != nil {
		return xerrors.Errorf("fail create coin account transaction: %v", err)
	}

	return nil
}

func (gac *goodAccounting) onLimitsChecker(ctx context.Context) {
	warmCoinLimit := 100
	resp, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
		CoinTypeID: gac.coininfo.ID,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get coin setting: %v", err)
		return
	}
	if resp.Info != nil {
		warmCoinLimit = int(resp.Info.WarmAccountCoinAmount)
	} else if gac.platformsetting != nil {
		price, err := currency.USDPrice(ctx, gac.coininfo.Name)
		if err == nil && price > 0 {
			warmCoinLimit = int(gac.platformsetting.WarmAccountUSDAmount / price)
		}
	}

	account, ok := gac.accounts[gac.coinsetting.UserOnlineAccountID]
	if !ok {
		logger.Sugar().Errorf("invalid user online account")
		return
	}

	_, ok = gac.accounts[gac.coinsetting.UserOfflineAccountID]
	if !ok {
		logger.Sugar().Errorf("invalid user offline account")
		return
	}

	resp1, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    gac.coininfo.Name,
		Address: account.Address,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get balance for good benefit account %v: %v [%v| %v %v]",
			gac.coinsetting.UserOnlineAccountID,
			err, gac.good.ID,
			gac.coininfo.Name,
			account.Address)
		return
	}

	if int(resp1.Info.Balance) > warmCoinLimit && int(resp1.Info.Balance)-warmCoinLimit > warmCoinLimit {
		_, err := grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
			Info: &billingpb.CoinAccountTransaction{
				AppID:              uuid.UUID{}.String(),
				UserID:             uuid.UUID{}.String(),
				FromAddressID:      gac.coinsetting.UserOnlineAccountID,
				ToAddressID:        gac.coinsetting.UserOfflineAccountID,
				CoinTypeID:         gac.coininfo.ID,
				Amount:             resp1.Info.Balance - float64(warmCoinLimit),
				Message:            fmt.Sprintf("warm transfer of %v at %v", gac.good.ID, time.Now()),
				ChainTransactionID: uuid.New().String(),
			},
		})
		if err != nil {
			logger.Sugar().Errorf("fail create coin account transaction: %v", err)
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
		from.Info.Address,
		to.Info.Address,
		coininfo.Info.Name)
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
			ChainTransactionID:   uuid.New().String(),
		},
	})
	if err != nil {
		logger.Sugar().Errorf("fail create platform benefit for good: %v [%v]", err, gac.good.ID)
		return
	}

	if gac.userUnits > 0 {
		if err := gac.onCreateBenefitTransaction(ctx, totalAmount, "user"); err != nil {
			logger.Sugar().Errorf("fail transfer: %v", err)
			return
		}
	}

	if gac.platformUnits > 0 {
		if err := gac.onCreateBenefitTransaction(ctx, totalAmount, "platform"); err != nil {
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

		lastBenefitTimestamp := uint32(time.Now().Unix()) / secondsInDay * secondsInDay

		_, err = grpc2.CreateUserBenefit(ctx, &billingpb.CreateUserBenefitRequest{
			Info: &billingpb.UserBenefit{
				AppID:                order.AppID,
				UserID:               order.UserID,
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

func onCreatedChecker(ctx context.Context) {
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

func onWaitChecker(ctx context.Context) {
	resp, err := grpc2.GetCoinAccountTransactionsByState(ctx, &billingpb.GetCoinAccountTransactionsByStateRequest{
		State: billingconst.CoinTransactionStateWait,
	})
	if err != nil {
		logger.Sugar().Errorf("fail get wait transactions: %v", err)
		return
	}

	for _, wait := range resp.Infos {
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

func onPayingChecker(ctx context.Context) {
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

func Run(ctx context.Context) { //nolint
	startAfter := (uint32(time.Now().Unix())/secondsInDay+1)*secondsInDay - secondsInHour*4
	startTimer := time.NewTimer(time.Duration(startAfter) * time.Second)
	<-startTimer.C

	ac := &accounting{
		scanTicker:             time.NewTicker(24 * time.Hour),
		transferTicker:         time.NewTicker(30 * time.Second),
		queryCoinInfo:          make(chan *goodAccounting),
		queryAccount:           make(chan *goodAccounting),
		queryAccountInfo:       make(chan *goodAccounting),
		queryBenefits:          make(chan *goodAccounting),
		querySpendTransactions: make(chan *goodAccounting),
		queryBalance:           make(chan *goodAccounting),
		queryOrders:            make(chan *goodAccounting),
		queryCompensates:       make(chan *goodAccounting),
		caculateUserBenefit:    make(chan *goodAccounting),
		checkLimits:            make(chan *goodAccounting),
		persistentResult:       make(chan *goodAccounting),
	}

	for {
		select {
		case <-ac.scanTicker.C:
			logger.Sugar().Infof("start query goods")
			ac.onQueryGoods(ctx)

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
			go func() { ac.checkLimits <- gac }()

		case gac := <-ac.checkLimits:
			gac.onLimitsChecker(ctx)
			go func() { ac.persistentResult <- gac }()

		case gac := <-ac.persistentResult:
			gac.onPersistentResult(ctx)

		case <-ac.transferTicker.C:
			onCreatedChecker(ctx)
			onWaitChecker(ctx)
			onPayingChecker(ctx)
		}
	}
}
