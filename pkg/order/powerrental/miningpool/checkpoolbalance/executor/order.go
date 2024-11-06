package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	fractionwithdrawalmwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/fractionwithdrawal"
	fractionwithdrawalrulemwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/fractionwithdrawalrule"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	fractionwithdrawalrulemwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/fractionwithdrawalrule"
	orderusermwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/orderuser"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/checkpoolbalance/types"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder

	fractionwithdrawalReqs []*fractionwithdrawalmwpb.FractionWithdrawalReq
	powerRentalOrderReq    *powerrentalordermwpb.PowerRentalOrderReq

	appPowerRental          *powerrentalgoodmwpb.PowerRental
	coinTypeIDs             []string
	balanceInfos            map[string]*orderusermwpb.BalanceInfo
	fractionwithdrawalRules map[string]*fractionwithdrawalrulemwpb.FractionWithdrawalRule
	orderUser               *orderusermwpb.OrderUser
	nextState               ordertypes.OrderState

	persistent chan interface{}
	done       chan interface{}
	notif      chan interface{}
}

func (h *orderHandler) getAppPowerRental(ctx context.Context) error {
	good, err := apppowerrentalmwcli.GetPowerRental(ctx, h.AppGoodID)
	if err != nil {
		return wlog.WrapError(err)
	}

	if good == nil {
		return wlog.Errorf("invalid powerrental")
	}

	h.appPowerRental = good
	return nil
}

func (h *orderHandler) checkAppPowerRental() error {
	if h.appPowerRental == nil {
		return wlog.Errorf("invalid powerrental")
	}
	if h.appPowerRental.State != goodtypes.GoodState_GoodStateReady {
		return wlog.Errorf("powerrental good not ready")
	}
	return nil
}

func (h *orderHandler) getCoinTypeIDs() error {
	for _, goodCoin := range h.appPowerRental.GoodCoins {
		h.coinTypeIDs = append(h.coinTypeIDs, goodCoin.CoinTypeID)
	}

	if len(h.coinTypeIDs) == 0 {
		return wlog.Errorf("have no goodcoins")
	}
	return nil
}

func (h *orderHandler) getOrderUser(ctx context.Context) error {
	if h.PowerRentalOrder.PoolOrderUserID == nil {
		return wlog.Errorf("invalid poolorderuserid")
	}

	info, err := orderusermwcli.GetOrderUser(ctx, *h.PowerRentalOrder.PoolOrderUserID)
	if err != nil {
		return wlog.WrapError(err)
	}

	h.orderUser = info
	return nil
}

func (h *orderHandler) getOrderUserBalanceInfos(ctx context.Context) error {
	if h.PowerRentalOrder.PoolOrderUserID == nil {
		return wlog.Errorf("invalid poolorderuserid")
	}

	h.balanceInfos = make(map[string]*orderusermwpb.BalanceInfo)
	for _, coinTypeID := range h.coinTypeIDs {
		info, err := orderusermwcli.GetOrderUserBalance(ctx, *h.PowerRentalOrder.PoolOrderUserID, coinTypeID)
		if err != nil {
			return wlog.WrapError(err)
		}
		if info == nil {
			return wlog.Errorf("invalid poolorderuserid")
		}
		h.balanceInfos[coinTypeID] = info
	}

	return nil
}

func (h *orderHandler) getFractionWithdrawalRules(ctx context.Context) error {
	if h.PowerRentalOrder.PoolOrderUserID == nil {
		return wlog.Errorf("invalid poolorderuserid")
	}

	h.fractionwithdrawalRules = make(map[string]*fractionwithdrawalrulemwpb.FractionWithdrawalRule)
	infos, _, err := fractionwithdrawalrulemwcli.GetFractionWithdrawalRules(ctx, &fractionwithdrawalrulemwpb.Conds{}, 0, 0)
	if err != nil {
		return wlog.WrapError(err)
	}

	for _, info := range infos {
		h.fractionwithdrawalRules[info.CoinTypeID] = info
	}

	return nil
}

func (h *orderHandler) checkOrderUserBalanceInfos() error {
	for _, cointypeid := range h.coinTypeIDs {
		if _, ok := h.balanceInfos[cointypeid]; !ok {
			return wlog.Errorf("cannot find balanceinfo in miningpool for cointypeid %v", cointypeid)
		}
	}

	return nil
}

func (h *orderHandler) checkFractionWithdrawalRules() error {
	for _, cointypeid := range h.coinTypeIDs {
		if _, ok := h.fractionwithdrawalRules[cointypeid]; !ok {
			return wlog.Errorf("cannot find fractionwithdrawalrule in miningpool for cointypeid %v", cointypeid)
		}
	}

	return nil
}

func (h *orderHandler) constructFractionWithdrawalReqs() error {
	for _, coinTypeID := range h.coinTypeIDs {
		balanceInfo := h.balanceInfos[coinTypeID]
		fractioRule := h.fractionwithdrawalRules[coinTypeID]

		stIncome, err := decimal.NewFromString(balanceInfo.EstimatedTodayIncome)
		if err != nil {
			return wlog.WrapError(err)
		}

		if stIncome.IsPositive() {
			return wlog.Errorf("still distributing income, waiting for the end of income distribution!")
		}

		balance, err := decimal.NewFromString(balanceInfo.Balance)
		if err != nil {
			return wlog.WrapError(err)
		}

		payoutThreshold, err := decimal.NewFromString(fractioRule.PayoutThreshold)
		if err != nil {
			return wlog.WrapError(err)
		}

		minAmount, err := decimal.NewFromString(fractioRule.LeastWithdrawalAmount)
		if err != nil {
			return wlog.WrapError(err)
		}

		if balance.Cmp(payoutThreshold) >= 0 {
			continue
		}

		if balance.Cmp(minAmount) >= 0 && h.nextState.String() == ordertypes.OrderState_OrderStateExpired.String() {
			h.nextState = ordertypes.OrderState_OrderStateCheckPoolBalance
		}

		h.fractionwithdrawalReqs = append(h.fractionwithdrawalReqs, &fractionwithdrawalmwpb.FractionWithdrawalReq{
			EntID:       func() *string { id := uuid.NewString(); return &id }(),
			AppID:       &h.orderUser.AppID,
			UserID:      &h.orderUser.UserID,
			OrderUserID: &h.orderUser.EntID,
			CoinTypeID:  &coinTypeID,
		})
	}

	return nil
}

func (h *orderHandler) constructUpdatePowerrentalOrder() {
	h.powerRentalOrderReq = &powerrentalordermwpb.PowerRentalOrderReq{
		ID:         &h.PowerRentalOrder.ID,
		EntID:      &h.PowerRentalOrder.EntID,
		OrderState: &h.nextState,
	}
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"PowerRentalOrder", h.PowerRentalOrder,
			"AdminSetCanceled", h.AdminSetCanceled,
			"UserSetCanceled", h.UserSetCanceled,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder:       h.PowerRentalOrder,
		FractionWithdrawalReqs: h.fractionwithdrawalReqs,
		PowerRentalOrderReq:    h.powerRentalOrderReq,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
	} else {
		asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.done)
	}
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.nextState = ordertypes.OrderState_OrderStateExpired

	var err error
	defer h.final(ctx, &err)

	if h.PowerRentalOrder.GoodStockMode != goodtypes.GoodStockMode_GoodStockByMiningPool {
		h.constructUpdatePowerrentalOrder()
		return nil
	}

	if err = h.getAppPowerRental(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.checkAppPowerRental(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.getCoinTypeIDs(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.getOrderUser(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.getFractionWithdrawalRules(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.getOrderUserBalanceInfos(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.checkOrderUserBalanceInfos(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.checkFractionWithdrawalRules(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.constructFractionWithdrawalReqs(); err != nil {
		return wlog.WrapError(err)
	}
	h.constructUpdatePowerrentalOrder()
	return nil
}
