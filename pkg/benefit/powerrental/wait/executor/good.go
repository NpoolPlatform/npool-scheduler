package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	goodstmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/good/ledger/statement"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	gbmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	goodstmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/good/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	common "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/wait/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/wait/types"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*types.FeedPowerRental
	*common.Handler
	persistent             chan interface{}
	notif                  chan interface{}
	done                   chan interface{}
	totalUnits             decimal.Decimal
	coinBenefitBalances    map[string]decimal.Decimal
	reservedAmount         decimal.Decimal
	totalInServiceUnits    decimal.Decimal
	totalBenefitOrderUnits decimal.Decimal
	startRewardAmount      decimal.Decimal
	nextStartRewardAmount  decimal.Decimal
	todayRewardAmount      decimal.Decimal
	todayUnitRewardAmount  decimal.Decimal
	userRewardAmount       decimal.Decimal
	platformRewardAmount   decimal.Decimal
	appOrderUnits          map[string]map[string]decimal.Decimal
	goodCoins              map[string]*coinmwpb.Coin
	coinRewards            []*types.CoinReward
	appGoods               map[string]map[string]*appgoodmwpb.Good
	goodCreatedAt          uint32
	techniqueFeeAppGoods   map[string]*appgoodmwpb.Good
	techniqueFeeAmount     decimal.Decimal
	userBenefitHotAccounts map[string]*pltfaccmwpb.Account
	goodBenefitAccounts    map[string]*gbmwpb.Account
	benefitOrderIDs        []uint32
	benefitOrderEntIDs     []string
	benefitResult          basetypes.Result
	benefitMessage         string
	notifiable             bool
	transferrable          bool
	benefitTimestamp       uint32
}

const (
	resultNotMining     = "Mining not start"
	resultMinimumReward = "Mining reward not transferred"
	resultInvalidStock  = "Good stock not consistent"
)

func (h *goodHandler) checkBenefitable() bool {
	if h.ServiceStartAt >= uint32(time.Now().Unix()) {
		h.benefitResult = basetypes.Result_Success
		h.benefitMessage = fmt.Sprintf(
			"%v (start at %v, now %v)",
			resultNotMining,
			time.Unix(int64(h.ServiceStartAt), 0),
			time.Now(),
		)
		h.notifiable = true
		return false
	}
	return true
}

func (h *goodHandler) getGoodCoins(ctx context.Context) (err error) {
	h.goodCoins, err = schedcommon.GetCoins(ctx, func() (coinTypeIDs []string) {
		for _, goodCoin := range h.GoodCoins {
			coinTypeIDs = append(coinTypeIDs, goodCoin.CoinTypeID)
		}
		return
	}())
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, goodCoin := range h.GoodCoins {
		if _, ok := h.goodCoins[goodCoin.CoinTypeID]; !ok {
			return wlog.Errorf("invalid goodcoin")
		}
	}
	return nil
}

func (h *goodHandler) checkBenefitBalances(ctx context.Context) error {
	h.coinBenefitBalances = map[string]decimal.Decimal{}
	for coinTypeID, goodBenefitAccount := range h.goodBenefitAccounts {
		coin, _ := h.goodCoins[coinTypeID]
		balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    coin.Name,
			Address: goodBenefitAccount.Address,
		})
		if err != nil {
			return fmt.Errorf(
				"%v (coin %v, address %v)",
				err,
				h.coin.Name,
				h.goodBenefitAccount.Address,
			)
		}
		if balance == nil {
			return fmt.Errorf(
				"invalid balance (coin %v, address %v)",
				h.coin.Name,
				h.goodBenefitAccount.Address,
			)
		}
		bal, err := decimal.NewFromString(balance.BalanceStr)
		if err != nil {
			return err
		}
		h.coinBenefitBalances[coinTypeID] = bal
	}
	return nil
}

func (h *goodHandler) orderBenefitable(order *ordermwpb.Order) bool {
	now := uint32(time.Now().Unix())
	switch order.PaymentState {
	case ordertypes.PaymentState_PaymentStateDone:
	case ordertypes.PaymentState_PaymentStateNoPayment:
	default:
		return false
	}

	// Here we must use endat for compensate
	if order.EndAt < now {
		return false
	}
	if order.StartAt > now {
		return false
	}
	if now < order.StartAt+uint32(h.BenefitInterval().Seconds()) {
		return false
	}

	return true
}

//nolint:gocognit
func (h *goodHandler) getOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:       &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			OrderState:   &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.OrderState_OrderStateInService)},
			BenefitState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitWait)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}
		for _, order := range orders {
			units, err := decimal.NewFromString(order.Units)
			if err != nil {
				return err
			}
			if !order.Simulate {
				h.totalInServiceUnits = h.totalInServiceUnits.Add(units)
			}
			if !h.orderBenefitable(order) {
				continue
			}
			h.benefitOrderIDs = append(h.benefitOrderIDs, order.ID)
			h.benefitOrderEntIDs = append(h.benefitOrderEntIDs, order.EntID)
			if order.Simulate {
				continue
			}
			h.totalBenefitOrderUnits = h.totalBenefitOrderUnits.Add(units)
			appGoodUnits, ok := h.appOrderUnits[order.AppID]
			if !ok {
				appGoodUnits = map[string]decimal.Decimal{
					order.AppGoodID: decimal.NewFromInt(0),
				}
			}
			appGoodUnits[order.AppGoodID] = appGoodUnits[order.AppGoodID].Add(units)
			h.appOrderUnits[order.AppID] = appGoodUnits
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) constructCoinRewards() error {
	for _, reward := range h.Rewards {
		startRewardAmount, err := decimal.NewFromString(reward.NextRewardStartAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		benefitBalance, ok := h.coinBenefitBalances[reward.CoinTypeID]
		if !ok {
			continue
		}
		todayRewardAmount := benefitBalance.Sub(startRewardAmount)
		coin, _ := h.goodCoins[reward.CoinTypeID]
		reservedAmount, err := decimal.NewFromString(coin.ReservedAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		if startRewardAmount.Cmp(decimal.NewFromInt(0)) == 0 {
			todayRewardAmount = todayRewardAmount.Sub(reservedAmount)
		}
		nextStartRewardAmount := h.benefitBalance
		todayUnitRewardAmount := todayRewardAmount.Div(h.totalUnits)
		userRewardAmount := todayRewardAmount.
			Mul(h.totalBenefitOrderUnits).
			Div(h.totalUnits)
		platformRewardAmount = todayRewardAmount.
			Sub(h.userRewardAmount)
	}
}

func (h *goodHandler) getGoodCreatedAt(ctx context.Context) error {
	good, err := goodmwcli.GetGood(ctx, h.EntID)
	if err != nil {
		return err
	}

	h.goodCreatedAt = good.CreatedAt
	return nil
}

func (h *goodHandler) getAppGoods(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			appGoods, ok := h.appGoods[good.AppID]
			if !ok {
				appGoods = map[string]*appgoodmwpb.Good{}
			}
			appGoods[good.EntID] = good
			h.appGoods[good.AppID] = appGoods
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) calculateTechniqueFeeLegacy() {
	for appID, appGoodUnits := range h.appOrderUnits {
		appGoods, ok := h.appGoods[appID]
		if !ok {
			continue
		}
		for appGoodID, units := range appGoodUnits {
			good, ok := appGoods[appGoodID]
			if !ok {
				continue
			}

			feeAmount := h.userRewardAmount.
				Mul(units).
				Div(h.totalBenefitOrderUnits).
				Mul(decimal.RequireFromString(good.TechnicalFeeRatio)).
				Div(decimal.NewFromInt(100))
			h.techniqueFeeAmount = h.techniqueFeeAmount.Add(feeAmount)
		}
	}
	h.userRewardAmount = h.userRewardAmount.Sub(h.techniqueFeeAmount)
}

func (h *goodHandler) _calculateTechniqueFee() error {
	for appID, appGoodUnits := range h.appOrderUnits {
		// For one good, event it's assign to multiple app goods,
		// we'll use the same technique fee app good due to good only can bind to one technique fee good
		techniqueFeeAppGood, ok := h.techniqueFeeAppGoods[appID]
		if !ok {
			continue
		}
		if techniqueFeeAppGood.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfit {
			continue
		}
		feePercent, err := decimal.NewFromString(techniqueFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}

		for _, units := range appGoodUnits {
			feeAmount := h.userRewardAmount.
				Mul(units).
				Div(h.totalBenefitOrderUnits).
				Mul(feePercent).
				Div(decimal.NewFromInt(100))
			h.techniqueFeeAmount = h.techniqueFeeAmount.Add(feeAmount)
		}
	}
	h.userRewardAmount = h.userRewardAmount.Sub(h.techniqueFeeAmount)
	return nil
}

func (h *goodHandler) getTechniqueFeeGoods(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	requireds := []*requiredmwpb.Required{}

	for {
		_requireds, _, err := requiredmwcli.GetRequireds(ctx, &requiredmwpb.Conds{
			MainGoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(_requireds) == 0 {
			break
		}
		requireds = append(requireds, _requireds...)
		offset += limit
	}

	offset = 0
	requiredGoodIDs := []string{}
	for _, required := range requireds {
		requiredGoodIDs = append(requiredGoodIDs, required.RequiredGoodID)
	}

	for {
		goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			GoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: requiredGoodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			if good.GoodType != goodtypes.GoodType_TechniqueServiceFee {
				continue
			}
			_, ok := h.techniqueFeeAppGoods[good.AppID]
			if ok {
				return fmt.Errorf("too many techniquefeegood")
			}
			h.techniqueFeeAppGoods[good.AppID] = good
		}
		offset += limit
	}

	return nil
}

func (h *goodHandler) calculateTechniqueFee() error {
	if h.totalBenefitOrderUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}
	if h.userRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	const legacyTechniqueFeeTimestamp = 1704009402
	if h.goodCreatedAt <= legacyTechniqueFeeTimestamp {
		h.calculateTechniqueFeeLegacy()
		return nil
	}

	return h._calculateTechniqueFee()
}

func (h *goodHandler) getUserBenefitHotAccounts(ctx context.Context) (err error) {
	h.userBenefitHotAccounts, err = schedcommon.GetCoinPlatformAccounts(
		ctx,
		basetypes.AccountUsedFor_UserBenefitHot,
		func() (conTypeIDs []string) {
			for coinTypeID, _ := range h.goodCoins {
				coinTypeIDs = append(coinTypeIDs, coinTypeID)
			}
			return
		}(),
	)
	return wlog.WrapError(err)
}

func (h *goodHandler) getGoodBenefitAccounts(ctx context.Context) (err error) {
	h.goodBenefitAccounts, err = schedcommon.GetGoodCoinBenefitAccounts(
		ctx,
		h.EntID,
		func() (conTypeIDs []string) {
			for coinTypeID, _ := range h.goodCoins {
				coinTypeIDs = append(coinTypeIDs, coinTypeID)
			}
			return
		}(),
	)
	return wlog.WrapError(err)
}

func (h *goodHandler) checkTransferrable() error {
	least, err := decimal.NewFromString(h.coin.LeastTransferAmount)
	if err != nil {
		return err
	}
	if least.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid leasttransferamount")
	}
	if h.todayRewardAmount.Cmp(least) <= 0 {
		h.benefitResult = basetypes.Result_Success
		h.benefitMessage = fmt.Sprintf(
			"%v (coin %v, address %v, amount %v)",
			resultMinimumReward,
			h.coin.Name,
			h.goodBenefitAccount.Address,
			h.todayRewardAmount,
		)
		h.notifiable = true
		return nil
	}
	h.transferrable = true
	return nil
}

func (h *goodHandler) validateInServiceUnits() error {
	goodInService, err := decimal.NewFromString(h.GoodInService)
	if err != nil {
		return err
	}

	inService := decimal.NewFromInt(0)
	for _, appGoods := range h.appGoods {
		for _, appGood := range appGoods {
			_goodInService, err := decimal.NewFromString(appGood.GoodInService)
			if err != nil {
				return err
			}
			if _goodInService.Cmp(goodInService) != 0 {
				return fmt.Errorf(
					"invalid inservice (good %v | %v, inservice %v != %v)",
					appGood.GoodName,
					appGood.ID,
					goodInService,
					_goodInService,
				)
			}
			_inService, err := decimal.NewFromString(appGood.AppGoodInService)
			if err != nil {
				return err
			}
			inService = inService.Add(_inService)
		}
	}
	if inService.Cmp(goodInService) != 0 {
		h.benefitResult = basetypes.Result_Fail
		h.benefitMessage = fmt.Sprintf(
			"%v (good %v | %v, in service %v != %v)",
			resultInvalidStock,
			h.Title,
			h.ID,
			inService,
			goodInService,
		)
		h.notifiable = true
		return fmt.Errorf("invalid inservice")
	}
	return nil
}

func (h *goodHandler) resolveBenefitTimestamp() {
	h.benefitTimestamp = h.TriggerBenefitTimestamp
	if h.benefitTimestamp == 0 {
		h.benefitTimestamp = h.BenefitTimestamp()
	}
}

func (h *goodHandler) checkGoodStatement(ctx context.Context) (bool, error) {
	exist, err := goodstmwcli.ExistGoodStatementConds(ctx, &goodstmwpb.Conds{
		GoodID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		BenefitDate: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.benefitTimestamp},
	})
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	return true, nil
}

//nolint:gocritic
func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Good", h.Good,
			"TodayRewardAmount", h.todayRewardAmount,
			"GoodBenefitAccount", h.goodBenefitAccount,
			"UserBenefitHotAccount", h.userBenefitHotAccount,
			"Transferrable", h.transferrable,
			"Notifiable", h.notifiable,
			"BenefitTimestamp", h.benefitTimestamp,
			"BenefitOrderIDs", len(h.benefitOrderIDs),
			"Error", *err,
		)
	}

	txExtra := fmt.Sprintf(
		`{"GoodID":"%v","Reward":"%v","UserReward":"%v","PlatformReward":"%v","TechniqueServiceFee":"%v"}`,
		h.EntID,
		h.todayRewardAmount,
		h.userRewardAmount,
		h.platformRewardAmount,
		h.techniqueFeeAmount,
	)

	persistentGood := &types.PersistentGood{
		Good:                  h.Good,
		BenefitOrderIDs:       h.benefitOrderIDs,
		TodayRewardAmount:     h.todayRewardAmount.String(),
		TodayUnitRewardAmount: h.todayUnitRewardAmount.String(),
		NextStartRewardAmount: h.nextStartRewardAmount.String(),
		FeeAmount:             decimal.NewFromInt(0).String(),
		Extra:                 txExtra,
		Transferrable:         h.transferrable,
		BenefitTimestamp:      h.benefitTimestamp,
		Error:                 *err,
	}

	if h.goodBenefitAccount != nil {
		persistentGood.GoodBenefitAccountID = h.goodBenefitAccount.AccountID
		persistentGood.GoodBenefitAddress = h.goodBenefitAccount.Address
	}
	if h.userBenefitHotAccount != nil {
		persistentGood.UserBenefitHotAccountID = h.userBenefitHotAccount.AccountID
		persistentGood.UserBenefitHotAddress = h.userBenefitHotAccount.Address
	}
	if h.notifiable {
		persistentGood.BenefitResult = h.benefitResult
		persistentGood.BenefitMessage = h.benefitMessage
		asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	}
	if *err != nil {
		persistentGood.BenefitResult = basetypes.Result_Fail
		persistentGood.BenefitMessage = (*err).Error()
		asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
		asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
		return
	}
	if h.todayRewardAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	h.appOrderUnits = map[string]map[string]decimal.Decimal{}
	h.appGoods = map[string]map[string]*appgoodmwpb.Good{}
	h.techniqueFeeAppGoods = map[string]*appgoodmwpb.Good{}
	h.benefitResult = basetypes.Result_Success

	var err error
	exist := false
	defer h.final(ctx, &err)

	h.resolveBenefitTimestamp()
	if exist, err = h.checkGoodStatement(ctx); err != nil || exist {
		return err
	}
	h.totalUnits, err = decimal.NewFromString(h.GoodTotal)
	if err != nil {
		return err
	}
	if h.totalUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		err = fmt.Errorf("invalid stock")
		return err
	}
	if benefitable := h.checkBenefitable(); !benefitable {
		return nil
	}
	if err = h.getGoodCoins(ctx); err != nil {
		return err
	}
	if err = h.getUserBenefitHotAccounts(ctx); err != nil {
		return err
	}
	if err = h.getGoodBenefitAccounts(ctx); err != nil {
		return err
	}
	if err = h.checkBenefitBalances(ctx); err != nil {
		return err
	}
	if err = h.getOrderUnits(ctx); err != nil {
		return err
	}
	if err = h.constructCoinRewards(); err != nil {

	}
	if err := h.getGoodCreatedAt(ctx); err != nil {
		return err
	}
	if err := h.getAppGoods(ctx); err != nil {
		return err
	}
	if err := h.validateInServiceUnits(); err != nil {
		return err
	}
	if err := h.getTechniqueFeeGoods(ctx); err != nil {
		return err
	}
	if err := h.calculateTechniqueFee(); err != nil {
		return err
	}
	if err = h.checkTransferrable(); err != nil {
		return err
	}

	return nil
}
