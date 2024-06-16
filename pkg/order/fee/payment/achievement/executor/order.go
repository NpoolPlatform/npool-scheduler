package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	goodcoinmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/coin"
	orderstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement/order"
	calculatemwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/calculate"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodcoinmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/coin"
	orderstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement/order"
	calculatemwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/calculate"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/payment/achievement/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*feeordermwpb.FeeOrder
	persistent      chan interface{}
	notif           chan interface{}
	done            chan interface{}
	orderStatements []*orderstatementmwpb.StatementReq
	goodMainCoin    *goodcoinmwpb.GoodCoin
}

func (h *orderHandler) checkAchievementExist(ctx context.Context) (bool, error) {
	return orderstatementmwcli.ExistStatementConds(ctx, &orderstatementmwpb.Conds{
		OrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.OrderID},
	})
}

func (h *orderHandler) getGoodMainCoin(ctx context.Context) (err error) {
	h.goodMainCoin, err = goodcoinmwcli.GetGoodCoinOnly(ctx, &goodcoinmwpb.Conds{
		GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ParentGoodID},
		Main:   &basetypes.BoolVal{Op: cruder.EQ, Value: true},
	})
	return wlog.WrapError(err)
}

func (h *orderHandler) calculateOrderStatements(ctx context.Context) (err error) {
	hasCommission := false
	switch h.OrderType {
	case ordertypes.OrderType_Normal:
		hasCommission = true
	case ordertypes.OrderType_Offline:
	case ordertypes.OrderType_Airdrop:
		return nil
	}
	h.orderStatements, err = calculatemwcli.Calculate(ctx, &calculatemwpb.CalculateRequest{
		AppID:            h.AppID,
		UserID:           h.UserID,
		GoodID:           h.GoodID,
		AppGoodID:        h.AppGoodID,
		OrderID:          h.EntID,
		GoodCoinTypeID:   h.goodMainCoin.CoinTypeID,
		Units:            decimal.NewFromInt(0).String(),
		PaymentAmountUSD: h.PaymentAmountUSD,
		GoodValueUSD:     h.PaymentGoodValueUSD,
		SettleType:       inspiretypes.SettleType_GoodOrderPayment,
		HasCommission:    hasCommission,
		OrderCreatedAt:   h.CreatedAt,
	})
	return wlog.WrapError(err)
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"FeeOrder", h.FeeOrder,
			"OrderStatements", h.orderStatements,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		FeeOrder:        h.FeeOrder,
		OrderStatements: h.orderStatements,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error
	var exist bool

	defer h.final(ctx, &err)

	if exist, err = h.checkAchievementExist(ctx); err != nil || exist {
		return err
	}
	if err = h.getGoodMainCoin(ctx); err != nil {
		return err
	}
	if err = h.calculateOrderStatements(ctx); err != nil {
		return err
	}

	return nil
}
