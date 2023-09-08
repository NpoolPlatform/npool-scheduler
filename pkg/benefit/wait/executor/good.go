package executor

import (
	"context"
	"fmt"
	"time"

	gbmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/goodbenefit"
	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	gbmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	common "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	*common.Handler
	persistent             chan interface{}
	notif                  chan interface{}
	done                   chan interface{}
	totalUnits             decimal.Decimal
	benefitBalance         decimal.Decimal
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
	coin                   *coinmwpb.Coin
	goods                  map[string]map[string]*appgoodmwpb.Good
	techniqueFeeAmount     decimal.Decimal
	userBenefitHotAccount  *pltfaccmwpb.Account
	goodBenefitAccount     *gbmwpb.Account
	benefitOrderIDs        []string
	benefitResult          basetypes.Result
	benefitMessage         string
	notifiable             bool
	transferrable          bool
}

const (
	resultNotMining     = "Mining not start"
	resultMinimumReward = "Mining reward not transferred"
)

func (h *goodHandler) checkBenefitable() (bool, error) {
	if h.StartAt >= uint32(time.Now().Unix()) {
		h.benefitResult = basetypes.Result_Success
		h.benefitMessage = fmt.Sprintf(
			"%v (start at %v, now %v)",
			resultNotMining,
			time.Unix(int64(h.StartAt), 0),
			time.Now(),
		)
		h.notifiable = true
		return false, nil
	}
	return true, nil
}

func (h *goodHandler) getCoin(ctx context.Context) error {
	coin, err := coinmwcli.GetCoin(ctx, h.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.coin = coin
	h.reservedAmount, err = decimal.NewFromString(h.coin.ReservedAmount)
	if err != nil {
		return err
	}
	return nil
}

func (h *goodHandler) checkBenefitBalance(ctx context.Context) error {
	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    h.coin.Name,
		Address: h.goodBenefitAccount.Address,
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
	h.benefitBalance = bal
	return nil
}

func (h *goodHandler) orderBenefitable(order *ordermwpb.Order) bool {
	now := uint32(time.Now().Unix())
	if order.PaymentState != ordertypes.PaymentState_PaymentStateDone {
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

func (h *goodHandler) getOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:       &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
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
			h.totalInServiceUnits = h.totalInServiceUnits.Add(units)
			if !h.orderBenefitable(order) {
				continue
			}
			h.totalBenefitOrderUnits = h.totalBenefitOrderUnits.Add(units)
			h.benefitOrderIDs = append(h.benefitOrderIDs, order.ID)
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

func (h *goodHandler) getAppGoods(ctx context.Context) error {
	appIDs := []string{}
	appGoodIDs := []string{}
	for appID, appGoodUnits := range h.appOrderUnits {
		appIDs = append(appIDs, appID)
		for appGoodID := range appGoodUnits {
			appGoodIDs = append(appGoodIDs, appGoodID)
		}
	}
	goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
		AppIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: appIDs},
		GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		IDs:    &basetypes.StringSliceVal{Op: cruder.IN, Value: appGoodIDs},
	}, int32(0), int32(len(appGoodIDs)))
	if err != nil {
		return err
	}
	for _, good := range goods {
		appGoods, ok := h.goods[good.AppID]
		if !ok {
			appGoods = map[string]*appgoodmwpb.Good{}
		}
		appGoods[good.ID] = good
		h.goods[good.AppID] = appGoods
	}
	return nil
}

func (h *goodHandler) calculateTechniqueFee() {
	if h.totalBenefitOrderUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		return
	}
	if h.userRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return
	}

	for appID, appGoodUnits := range h.appOrderUnits {
		appGoods, ok := h.goods[appID]
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

func (h *goodHandler) getUserBenefitHotAccount(ctx context.Context) error {
	account, err := pltfaccmwcli.GetAccountOnly(ctx, &pltfaccmwpb.Conds{
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.CoinTypeID},
		UsedFor:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(basetypes.AccountUsedFor_UserBenefitHot)},
		Backup:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:     &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:    &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Locked:     &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid account")
	}
	h.userBenefitHotAccount = account
	return nil
}

func (h *goodHandler) getGoodBenefitAccount(ctx context.Context) error {
	account, err := gbmwcli.GetAccountOnly(ctx, &gbmwpb.Conds{
		GoodID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		Backup:  &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Active:  &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:  &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		Blocked: &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid account")
	}
	h.goodBenefitAccount = account
	return nil
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

//nolint:gocritic
func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Good", h.Good,
			"TodayRewardAmount", h.todayRewardAmount,
			"GoodBenefitAccount", h.goodBenefitAccount,
			"UserBenefitHotAccount", h.userBenefitHotAccount,
			"Notifiable", h.notifiable,
			"Error", *err,
		)
	}

	txExtra := fmt.Sprintf(
		`{"GoodID":"%v","Reward":"%v","UserReward":"%v","PlatformReward":"%v","TechniqueServiceFee":"%v"}`,
		h.ID,
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
		BenefitTimestamp:      h.BenefitTimestamp(),
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
	}
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	h.appOrderUnits = map[string]map[string]decimal.Decimal{}
	h.goods = map[string]map[string]*appgoodmwpb.Good{}
	h.benefitResult = basetypes.Result_Success

	var err error
	defer h.final(ctx, &err)

	h.totalUnits, err = decimal.NewFromString(h.GoodTotal)
	if err != nil {
		return err
	}
	if h.totalUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		err = fmt.Errorf("invalid stock")
		return err
	}
	h.startRewardAmount, err = decimal.NewFromString(h.NextRewardStartAmount)
	if err != nil {
		return err
	}
	if benefitable, err := h.checkBenefitable(); err != nil || !benefitable {
		return err
	}
	if err = h.getCoin(ctx); err != nil {
		return err
	}
	if err = h.getUserBenefitHotAccount(ctx); err != nil {
		return err
	}
	if err = h.getGoodBenefitAccount(ctx); err != nil {
		return err
	}
	if err = h.checkBenefitBalance(ctx); err != nil {
		return err
	}
	if err = h.getOrderUnits(ctx); err != nil {
		return err
	}
	h.todayRewardAmount = h.benefitBalance.Sub(h.startRewardAmount)
	if h.startRewardAmount.Cmp(decimal.NewFromInt(0)) == 0 {
		h.todayRewardAmount = h.todayRewardAmount.Sub(h.reservedAmount)
	}
	h.nextStartRewardAmount = h.benefitBalance
	h.todayUnitRewardAmount = h.todayRewardAmount.Div(h.totalUnits)
	h.userRewardAmount = h.todayRewardAmount.
		Mul(h.totalBenefitOrderUnits).
		Div(h.totalUnits)
	h.platformRewardAmount = h.todayRewardAmount.
		Sub(h.userRewardAmount)
	if err := h.getAppGoods(ctx); err != nil {
		return err
	}
	h.calculateTechniqueFee()
	if err = h.checkTransferrable(); err != nil {
		return err
	}

	return nil
}
