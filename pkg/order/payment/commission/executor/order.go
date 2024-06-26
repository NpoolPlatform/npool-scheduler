package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	calculatemwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/calculate"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	calculatemwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/calculate"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/commission/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent       chan interface{}
	notif            chan interface{}
	done             chan interface{}
	paymentAmount    decimal.Decimal
	statements       []*achievementstatementmwpb.Statement
	ledgerStatements []*ledgerstatementmwpb.StatementReq
	childOrders      []*ordermwpb.Order
	goodValue        decimal.Decimal
	goodValueUSD     decimal.Decimal
}

func (h *orderHandler) getChildOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			PaymentType:   &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.PaymentType_PayWithParentOrder)},
			ParentOrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}
		h.childOrders = append(h.childOrders, orders...)
		offset += limit
	}
	return nil
}

func (h *orderHandler) calculateGoodValue() error {
	for _, order := range h.childOrders {
		amount, err := decimal.NewFromString(order.GoodValue)
		if err != nil {
			return err
		}
		h.goodValue = h.goodValue.Add(amount)
	}

	amount, err := decimal.NewFromString(h.GoodValue)
	if err != nil {
		return err
	}
	h.goodValue = h.goodValue.Add(amount)

	currency, err := decimal.NewFromString(h.CoinUSDCurrency)
	if err != nil {
		return err
	}

	h.goodValueUSD = h.goodValue.Mul(currency)

	return nil
}

func (h *orderHandler) calculateAchievementStatements(ctx context.Context) error {
	hasCommission := false

	switch h.OrderType {
	case ordertypes.OrderType_Normal:
		hasCommission = true
	case ordertypes.OrderType_Offline:
	case ordertypes.OrderType_Airdrop:
	}

	if !h.MultiPaymentCoins {
		statements, err := calculatemwcli.Calculate(ctx, &calculatemwpb.CalculateRequest{
			AppID:                  h.AppID,
			UserID:                 h.UserID,
			GoodID:                 h.GoodID,
			AppGoodID:              h.AppGoodID,
			OrderID:                h.EntID,
			PaymentID:              h.PaymentID,
			CoinTypeID:             h.CoinTypeID,
			PaymentCoinTypeID:      h.PaymentCoinTypeID,
			PaymentCoinUSDCurrency: h.CoinUSDCurrency,
			Units:                  h.Units,
			PaymentAmount:          h.PaymentAmount,
			GoodValue:              h.goodValue.String(),
			GoodValueUSD:           h.goodValueUSD.String(),
			SettleType:             inspiretypes.SettleType_GoodOrderPayment,
			HasCommission:          hasCommission,
			OrderCreatedAt:         h.CreatedAt,
		})
		if err != nil {
			return err
		}
		h.statements = statements
		return nil
	}

	for _, paymentAmount := range h.PaymentAmounts {
		amount, err := decimal.NewFromString(paymentAmount.Amount)
		if err != nil {
			return err
		}
		currency, err := decimal.NewFromString(paymentAmount.USDCurrency)
		if err != nil {
			return err
		}
		usdAmount := amount.Mul(currency)
		usdAmountStr := usdAmount.String()

		statements, err := calculatemwcli.Calculate(ctx, &calculatemwpb.CalculateRequest{
			AppID:                  h.AppID,
			UserID:                 h.UserID,
			GoodID:                 h.GoodID,
			AppGoodID:              h.AppGoodID,
			OrderID:                h.EntID,
			PaymentID:              h.PaymentID,
			CoinTypeID:             h.CoinTypeID,
			PaymentCoinTypeID:      paymentAmount.CoinTypeID,
			PaymentCoinUSDCurrency: paymentAmount.USDCurrency,
			Units:                  h.Units,
			PaymentAmount:          paymentAmount.Amount,
			GoodValue:              paymentAmount.Amount,
			GoodValueUSD:           usdAmountStr,
			SettleType:             inspiretypes.SettleType_GoodOrderPayment,
			HasCommission:          hasCommission,
			OrderCreatedAt:         h.CreatedAt,
		})
		if err != nil {
			return err
		}
		h.statements = append(h.statements, statements...)
	}

	return nil
}

func (h *orderHandler) calculateLedgerStatements() error {
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_Commission

	for _, statement := range h.statements {
		commission, err := decimal.NewFromString(statement.Commission)
		if err != nil {
			return err
		}
		if commission.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}
		ioExtra := fmt.Sprintf(
			`{"PaymentID":"%v","OrderID":"%v","DirectContributorID":"%v","OrderUserID":"%v","InspireAppConfigID":"%v","CommissionConfigID":"%v","CommissionConfigType":"%v"}`,
			h.PaymentID,
			h.EntID,
			statement.DirectContributorID,
			h.UserID,
			statement.AppConfigID,
			statement.CommissionConfigID,
			statement.CommissionConfigType,
		)

		h.ledgerStatements = append(h.ledgerStatements, &ledgerstatementmwpb.StatementReq{
			AppID:      &h.AppID,
			UserID:     &statement.UserID,
			CoinTypeID: &statement.PaymentCoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &statement.Commission,
			IOExtra:    &ioExtra,
		})
	}
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:            h.Order,
		LedgerStatements: h.ledgerStatements,
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

	defer h.final(ctx, &err)

	if h.paymentAmount, err = decimal.NewFromString(h.PaymentAmount); err != nil {
		return err
	}
	if err = h.getChildOrders(ctx); err != nil {
		return err
	}
	if err = h.calculateGoodValue(); err != nil {
		return err
	}
	if err = h.calculateAchievementStatements(ctx); err != nil {
		return err
	}
	if err = h.calculateLedgerStatements(); err != nil {
		return err
	}

	return nil
}
