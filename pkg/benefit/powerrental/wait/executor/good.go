package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	appfeemwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/fee"
	requiredappgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good/required"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	goodstatementmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/good/ledger/statement"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodbenefitmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"
	platformaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	appfeemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/fee"
	requiredappgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/required"
	apppowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	goodstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/good/ledger/statement"
	outofgasmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/outofgas"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	common "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/wait/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/wait/types"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	outofgasmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/outofgas"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type coinReward struct {
	types.CoinReward
	todayRewardAmount  decimal.Decimal
	userRewardAmount   decimal.Decimal
	techniqueFeeAmount decimal.Decimal
}

type goodHandler struct {
	*types.FeedPowerRental
	*common.Handler
	persistent             chan interface{}
	notif                  chan interface{}
	done                   chan interface{}
	totalUnits             decimal.Decimal
	coinBenefitBalances    map[string]decimal.Decimal
	totalBenefitOrderUnits decimal.Decimal
	appOrderUnits          map[string]map[string]decimal.Decimal
	goodCoins              map[string]*coinmwpb.Coin
	coinRewards            []*coinReward
	appPowerRentals        map[string]map[string]*apppowerrentalmwpb.PowerRental
	requiredAppFees        []*requiredappgoodmwpb.Required
	techniqueFees          map[string]*appfeemwpb.Fee
	userBenefitHotAccounts map[string]*platformaccountmwpb.Account
	goodBenefitAccounts    map[string]*goodbenefitmwpb.Account
	benefitOrderIDs        []uint32
	orderOutOfGases        map[string]*outofgasmwpb.OutOfGas
	benefitResult          basetypes.Result
	benefitMessage         string
	notifiable             bool
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

func (h *goodHandler) getBenefitBalances(ctx context.Context) error {
	h.coinBenefitBalances = map[string]decimal.Decimal{}
	for coinTypeID, goodBenefitAccount := range h.goodBenefitAccounts {
		coin, ok := h.goodCoins[coinTypeID]
		if !ok {
			return wlog.Errorf("invalid coin")
		}
		balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    coin.Name,
			Address: goodBenefitAccount.Address,
		})
		if err != nil {
			return wlog.Errorf(
				"%v (coin %v, address %v)",
				err,
				coin.Name,
				goodBenefitAccount.Address,
			)
		}
		if balance == nil {
			return wlog.Errorf(
				"invalid balance (coin %v, address %v)",
				coin.Name,
				goodBenefitAccount.Address,
			)
		}
		bal, err := decimal.NewFromString(balance.BalanceStr)
		if err != nil {
			return wlog.WrapError(err)
		}
		h.coinBenefitBalances[coinTypeID] = bal
	}
	return nil
}

func (h *goodHandler) orderBenefitable(order *powerrentalordermwpb.PowerRentalOrder) bool {
	if order.Simulate {
		return false
	}

	if _, ok := h.orderOutOfGases[order.OrderID]; ok {
		return false
	}

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

func (h *goodHandler) getOutOfGasesWithOrderIDs(ctx context.Context, orderIDs []string) error {
	h.orderOutOfGases = map[string]*outofgasmwpb.OutOfGas{}
	infos, _, err := outofgasmwcli.GetOutOfGases(ctx, &outofgasmwpb.Conds{
		OrderIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: orderIDs},
		EndAt:    &basetypes.Uint32Val{Op: cruder.EQ, Value: 0},
	}, 0, int32(len(orderIDs)))
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, info := range infos {
		h.orderOutOfGases[info.OrderID] = info
	}
	return nil
}

//nolint:gocognit
func (h *goodHandler) getOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.appOrderUnits = map[string]map[string]decimal.Decimal{}

	for {
		orders, _, err := powerrentalordermwcli.GetPowerRentalOrders(ctx, &powerrentalordermwpb.Conds{
			GoodID:       &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
			OrderState:   &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.OrderState_OrderStateInService)},
			BenefitState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.BenefitState_BenefitWait)},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(orders) == 0 {
			break
		}
		if err := h.getOutOfGasesWithOrderIDs(ctx, func() (orderIDs []string) {
			for _, order := range orders {
				orderIDs = append(orderIDs, order.OrderID)
			}
			return
		}()); err != nil {
			return wlog.WrapError(err)
		}
		for _, order := range orders {
			units, err := decimal.NewFromString(order.Units)
			if err != nil {
				return wlog.WrapError(err)
			}
			if !h.orderBenefitable(order) {
				if order.Simulate {
					h.benefitOrderIDs = append(h.benefitOrderIDs, order.ID)
				}
				continue
			}
			h.benefitOrderIDs = append(h.benefitOrderIDs, order.ID)
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

//nolint:gocognit
func (h *goodHandler) constructCoinRewards() error {
	for _, reward := range h.Rewards {
		startRewardAmount, err := decimal.NewFromString(reward.NextRewardStartAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		benefitBalance, ok := h.coinBenefitBalances[reward.CoinTypeID]
		if !ok {
			return wlog.Errorf("Invalid benefit balance")
		}
		todayRewardAmount := benefitBalance.Sub(startRewardAmount)
		coin, ok := h.goodCoins[reward.CoinTypeID]
		if !ok {
			return wlog.Errorf("Invalid goodcoin")
		}
		reservedAmount, err := decimal.NewFromString(coin.ReservedAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		if startRewardAmount.Equal(decimal.NewFromInt(0)) {
			todayRewardAmount = todayRewardAmount.Sub(reservedAmount)
		}
		if todayRewardAmount.LessThan(decimal.NewFromInt(0)) {
			todayRewardAmount = decimal.NewFromInt(0)
		}

		nextRewardStartAmount := startRewardAmount
		if todayRewardAmount.GreaterThan(decimal.NewFromInt(0)) {
			nextRewardStartAmount = benefitBalance
		}
		userRewardAmount := todayRewardAmount.
			Mul(h.totalBenefitOrderUnits).
			Div(h.totalUnits)

		coinReward := &coinReward{
			CoinReward: types.CoinReward{
				CoinTypeID:            reward.CoinTypeID,
				Amount:                todayRewardAmount.String(),
				NextRewardStartAmount: nextRewardStartAmount.String(),
			},
			todayRewardAmount: todayRewardAmount,
			userRewardAmount:  userRewardAmount,
		}

		if reward.MainCoin {
			if err := h.calculateTechniqueFee(coinReward); err != nil {
				return wlog.WrapError(err)
			}
		}
		platformRewardAmount := todayRewardAmount.
			Sub(userRewardAmount)

		goodBenefitAccount, ok := h.goodBenefitAccounts[reward.CoinTypeID]
		if ok {
			coinReward.GoodBenefitAccountID = goodBenefitAccount.AccountID
			coinReward.GoodBenefitAddress = goodBenefitAccount.Address
		}
		userBenefitHotAccount, ok := h.userBenefitHotAccounts[reward.CoinTypeID]
		if ok {
			coinReward.UserBenefitHotAccountID = userBenefitHotAccount.AccountID
			coinReward.UserBenefitHotAddress = userBenefitHotAccount.Address
		}
		coinReward.Extra = fmt.Sprintf(
			`{"GoodID":"%v","Reward":"%v","UserReward":"%v","PlatformReward":"%v","TechniqueServiceFee":"%v"}`,
			h.GoodID,
			todayRewardAmount,
			userRewardAmount,
			platformRewardAmount,
			coinReward.techniqueFeeAmount,
		)
		if err := h.checkTransferrable(coinReward); err != nil {
			return wlog.WrapError(err)
		}
		h.coinRewards = append(h.coinRewards, coinReward)
	}
	return nil
}

func (h *goodHandler) getAppPowerRentals(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.appPowerRentals = map[string]map[string]*apppowerrentalmwpb.PowerRental{}

	for {
		goods, _, err := apppowerrentalmwcli.GetPowerRentals(ctx, &apppowerrentalmwpb.Conds{
			GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			appPowerRentals, ok := h.appPowerRentals[good.AppID]
			if !ok {
				appPowerRentals = map[string]*apppowerrentalmwpb.PowerRental{}
			}
			appPowerRentals[good.AppGoodID] = good
			h.appPowerRentals[good.AppID] = appPowerRentals
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) calculateTechniqueFeeLegacy(reward *coinReward) {
	for appID, appGoodUnits := range h.appOrderUnits {
		appPowerRentals, ok := h.appPowerRentals[appID]
		if !ok {
			continue
		}
		for appGoodID, units := range appGoodUnits {
			good, ok := appPowerRentals[appGoodID]
			if !ok {
				continue
			}

			feeAmount := reward.userRewardAmount.
				Mul(units).
				Div(h.totalBenefitOrderUnits).
				Mul(decimal.RequireFromString(good.TechniqueFeeRatio)).
				Div(decimal.NewFromInt(100))
			reward.techniqueFeeAmount = reward.techniqueFeeAmount.Add(feeAmount)
		}
	}
	reward.userRewardAmount = reward.userRewardAmount.Sub(reward.techniqueFeeAmount)
}

func (h *goodHandler) _calculateTechniqueFee(reward *coinReward) error {
	for appID, appGoodUnits := range h.appOrderUnits {
		// For one good, event it's assign to multiple app goods,
		// we'll use the same technique fee app good due to good only can bind to one technique fee good
		techniqueFee, ok := h.techniqueFees[appID]
		if !ok {
			continue
		}
		if techniqueFee.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfitPercent {
			continue
		}
		feePercent, err := decimal.NewFromString(techniqueFee.UnitValue)
		if err != nil {
			return wlog.WrapError(err)
		}

		for _, units := range appGoodUnits {
			feeAmount := reward.userRewardAmount.
				Mul(units).
				Div(h.totalBenefitOrderUnits).
				Mul(feePercent).
				Div(decimal.NewFromInt(100))
			reward.techniqueFeeAmount = reward.techniqueFeeAmount.Add(feeAmount)
		}
	}
	reward.userRewardAmount = reward.userRewardAmount.Sub(reward.techniqueFeeAmount)
	return nil
}

func (h *goodHandler) getRequiredTechniqueFees(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		requireds, _, err := requiredappgoodmwcli.GetRequireds(ctx, &requiredappgoodmwpb.Conds{
			MainAppGoodIDs: &basetypes.StringSliceVal{
				Op: cruder.IN, Value: func() (appGoodIDs []string) {
					for _, appPowerRentals := range h.appPowerRentals {
						for _, appPowerRental := range appPowerRentals {
							appGoodIDs = append(appGoodIDs, appPowerRental.AppGoodID)
						}
					}
					return
				}(),
			},
			RequiredGoodType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(goodtypes.GoodType_TechniqueServiceFee)},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(requireds) == 0 {
			return nil
		}
		h.requiredAppFees = append(h.requiredAppFees, requireds...)
		offset += limit
	}
}

func (h *goodHandler) getAppTechniqueFees(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	h.techniqueFees = map[string]*appfeemwpb.Fee{}

	for {
		goods, _, err := appfeemwcli.GetFees(ctx, &appfeemwpb.Conds{
			AppGoodIDs: &basetypes.StringSliceVal{
				Op: cruder.IN, Value: func() (appGoodIDs []string) {
					for _, requiredAppFee := range h.requiredAppFees {
						appGoodIDs = append(appGoodIDs, requiredAppFee.RequiredAppGoodID)
					}
					return
				}(),
			},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			if good.GoodType != goodtypes.GoodType_TechniqueServiceFee {
				continue
			}
			_, ok := h.techniqueFees[good.AppID]
			if ok {
				return wlog.Errorf("too many techniquefeegood")
			}
			h.techniqueFees[good.AppID] = good
		}
		offset += limit
	}

	return nil
}

func (h *goodHandler) calculateTechniqueFee(reward *coinReward) error {
	if h.totalBenefitOrderUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}
	if reward.userRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	if h.GoodType == goodtypes.GoodType_LegacyPowerRental {
		h.calculateTechniqueFeeLegacy(reward)
		return nil
	}

	return h._calculateTechniqueFee(reward)
}

func (h *goodHandler) getUserBenefitHotAccounts(ctx context.Context) (err error) {
	h.userBenefitHotAccounts, err = schedcommon.GetCoinPlatformAccounts(
		ctx,
		basetypes.AccountUsedFor_UserBenefitHot,
		func() (coinTypeIDs []string) {
			for coinTypeID := range h.goodCoins {
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
		h.GoodID,
		func() (coinTypeIDs []string) {
			for coinTypeID := range h.goodCoins {
				coinTypeIDs = append(coinTypeIDs, coinTypeID)
			}
			return
		}(),
	)
	return wlog.WrapError(err)
}

func (h *goodHandler) checkTransferrable(reward *coinReward) error {
	coin, ok := h.goodCoins[reward.CoinTypeID]
	if !ok {
		return nil
	}
	least, err := decimal.NewFromString(coin.LeastTransferAmount)
	if err != nil {
		return err
	}
	if least.Cmp(decimal.NewFromInt(0)) <= 0 {
		return wlog.Errorf("invalid leasttransferamount")
	}
	if reward.todayRewardAmount.Cmp(least) <= 0 {
		reward.BenefitMessage = fmt.Sprintf(
			"%v (coin %v, address %v, amount %v)",
			resultMinimumReward,
			coin.Name,
			reward.GoodBenefitAddress,
			reward.todayRewardAmount,
		)
		h.notifiable = true
		return nil
	}
	reward.Transferrable = true
	return nil
}

func (h *goodHandler) validateInServiceUnits() error {
	goodInService, err := decimal.NewFromString(h.GoodInService)
	if err != nil {
		return wlog.WrapError(err)
	}

	inService := decimal.NewFromInt(0)
	for _, appPowerRentals := range h.appPowerRentals {
		for _, appPowerRental := range appPowerRentals {
			_inService, err := decimal.NewFromString(appPowerRental.AppGoodInService)
			if err != nil {
				return wlog.WrapError(err)
			}
			inService = inService.Add(_inService)
		}
	}
	if inService.Cmp(goodInService) != 0 {
		h.benefitResult = basetypes.Result_Fail
		h.benefitMessage = fmt.Sprintf(
			"%v (good %v [%v], in service %v != %v)",
			resultInvalidStock,
			h.Name,
			h.GoodID,
			inService,
			goodInService,
		)
		h.notifiable = true
		return wlog.Errorf("invalid inservice")
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
	exist, err := goodstatementmwcli.ExistGoodStatementConds(ctx, &goodstatementmwpb.Conds{
		GoodID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
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
			"PowerRental", h.PowerRental,
			"Notifiable", h.notifiable,
			"BenefitTimestamp", h.benefitTimestamp,
			"BenefitOrderIDs", len(h.benefitOrderIDs),
			"CoinRewards", h.coinRewards,
			"BenefitMessage", h.benefitMessage,
			"BenefitResult", h.benefitResult,
			"Error", *err,
		)
	}

	persistentGood := &types.PersistentPowerRental{
		PowerRental:     h.PowerRental,
		BenefitOrderIDs: h.benefitOrderIDs,
		CoinRewards: func() (rewards []*types.CoinReward) {
			for _, reward := range h.coinRewards {
				rewards = append(rewards, &reward.CoinReward)
			}
			return
		}(),
		BenefitTimestamp: h.benefitTimestamp,
		Error:            *err,
	}

	if h.notifiable {
		persistentGood.BenefitResult = h.benefitResult
		for _, reward := range persistentGood.CoinRewards {
			if reward.BenefitMessage == "" {
				reward.BenefitMessage = h.benefitMessage
			}
		}
		asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	}
	if *err != nil {
		persistentGood.BenefitResult = basetypes.Result_Fail
		for _, reward := range persistentGood.CoinRewards {
			reward.BenefitMessage = (*err).Error()
		}
		asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
		asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
		return
	}
	if len(h.coinRewards) > 0 {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	h.benefitResult = basetypes.Result_Success

	var err error
	exist := false
	defer h.final(ctx, &err)

	h.resolveBenefitTimestamp()
	if exist, err = h.checkGoodStatement(ctx); err != nil || exist {
		return wlog.WrapError(err)
	}
	h.totalUnits, err = decimal.NewFromString(h.GoodTotal)
	if err != nil {
		return wlog.WrapError(err)
	}
	if h.totalUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		err = wlog.Errorf("invalid stock")
		return wlog.WrapError(err)
	}
	if benefitable := h.checkBenefitable(); !benefitable {
		return nil
	}
	if err = h.getGoodCoins(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.getUserBenefitHotAccounts(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.getGoodBenefitAccounts(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.getBenefitBalances(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.getOrderUnits(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err := h.getAppPowerRentals(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err := h.validateInServiceUnits(); err != nil {
		return wlog.WrapError(err)
	}
	if err := h.getRequiredTechniqueFees(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err := h.getAppTechniqueFees(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.constructCoinRewards(); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
