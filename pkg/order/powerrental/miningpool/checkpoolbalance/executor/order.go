package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	fractionmwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/fraction"
	fractionrulemwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/fractionrule"
	orderusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/orderuser"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	fractionrulemwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/fractionrule"
	orderusermwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/orderuser"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/miningpool/checkpoolbalance/types"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	appPowerRental *powerrentalgoodmwpb.PowerRental

	coinTypeIDs   []string
	balanceInfos  map[string]*orderusermwpb.BalanceInfo
	fractionRules map[string]*fractionrulemwpb.FractionRule
	orderUser     *orderusermwpb.OrderUser

	fractionReqs []*fractionmwpb.FractionReq
	nextState    *ordertypes.OrderState
	persistent   chan interface{}
	done         chan interface{}
	notif        chan interface{}
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

func (h *orderHandler) getFractionRules(ctx context.Context) error {
	if h.PowerRentalOrder.PoolOrderUserID == nil {
		return wlog.Errorf("invalid poolorderuserid")
	}

	h.fractionRules = make(map[string]*fractionrulemwpb.FractionRule)
	infos, _, err := fractionrulemwcli.GetFractionRules(ctx, &fractionrulemwpb.Conds{}, 0, 0)
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, info := range infos {
		h.fractionRules[info.CoinTypeID] = info
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

func (h *orderHandler) checkFractionRules() error {
	for _, cointypeid := range h.coinTypeIDs {
		if _, ok := h.fractionRules[cointypeid]; !ok {
			return wlog.Errorf("cannot find fractionrule in miningpool for cointypeid %v", cointypeid)
		}
	}
	return nil
}

func (h *orderHandler) constructFractionReqs() error {
	h.nextState = ordertypes.OrderState_OrderStateExpired.Enum()

	for _, coinTypeID := range h.coinTypeIDs {
		balanceInfo := h.balanceInfos[coinTypeID]
		fractioRule := h.fractionRules[coinTypeID]

		if balanceInfo.EstimatedTodayIncome != 0 {
			return wlog.Errorf("still distributing income, waiting for the end of income distribution!")
		}

		balance := decimal.NewFromFloat(balanceInfo.Balance)
		payoutThreshold, err := decimal.NewFromString(fractioRule.PayoutThreshold)
		if err != nil {
			return wlog.WrapError(err)
		}

		minAmount, err := decimal.NewFromString(fractioRule.MinAmount)
		if err != nil {
			return wlog.WrapError(err)
		}

		if balance.Cmp(payoutThreshold) >= 0 {
			continue
		}

		if balance.Cmp(minAmount) >= 0 && h.nextState.String() == ordertypes.OrderState_OrderStateExpired.String() {
			h.nextState = ordertypes.OrderState_OrderStateCheckPoolBalance.Enum()
		}

		h.fractionReqs = append(h.fractionReqs, &fractionmwpb.FractionReq{
			EntID:       func() *string { id := uuid.NewString(); return &id }(),
			AppID:       &h.orderUser.AppID,
			UserID:      &h.orderUser.UserID,
			OrderUserID: &h.orderUser.EntID,
			CoinTypeID:  &coinTypeID,
		})

	}

	return nil
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
		PowerRentalOrder: h.PowerRentalOrder,
		FractionReqs:     h.fractionReqs,
		NextState:        h.nextState,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
	}

	asyncfeed.AsyncFeed(ctx, h.PowerRentalOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

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

	if err = h.getFractionRules(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.getOrderUserBalanceInfos(ctx); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.checkOrderUserBalanceInfos(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.checkFractionRules(); err != nil {
		return wlog.WrapError(err)
	}

	if err = h.constructFractionReqs(); err != nil {
		return wlog.WrapError(err)
	}
	return nil
}
