package executor

import (
	"context"
	"encoding/json"
	"fmt"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	calculatemwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/calculate"
	ledgerstatementmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger/statement"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	calculatemwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/calculate"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/achievement/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent                chan interface{}
	notif                     chan interface{}
	done                      chan interface{}
	achievementStatements     []*achievementstatementmwpb.StatementReq
	statements                []*achievementstatementmwpb.Statement
	paymentAmount             decimal.Decimal
	childOrders               []*ordermwpb.Order
	goodValue                 decimal.Decimal
	goodValueUSD              decimal.Decimal
	usdtTrc20Coin             *coinmwpb.Coin
	goodCoin                  *coinmwpb.Coin
	orderCommissionStatements map[string]*ledgerstatementmwpb.Statement
}

type b struct {
	PaymentID            string
	OrderID              string
	DirectContributorID  string
	OrderUserID          string
	InspireAppConfigID   string
	CommissionConfigID   string
	CommissionConfigType string
}

func (h *orderHandler) getOrdersCommissionStatements(ctx context.Context) error {
	h.orderCommissionStatements = map[string]*ledgerstatementmwpb.Statement{}
	offset := int32(0)
	limit := constant.DefaultRowLimit
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_Commission

	for {
		statements, _, err := ledgerstatementmwcli.GetStatements(ctx, &ledgerstatementmwpb.Conds{
			IOExtra:    &basetypes.StringVal{Op: cruder.LIKE, Value: h.EntID},
			AppID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
			CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: h.PaymentCoinTypeID},
			IOType:     &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ioType)},
			IOSubType:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ioSubType)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(statements) == 0 {
			break
		}
		for _, statement := range statements {
			var _b b
			if err := json.Unmarshal([]byte(statement.IOExtra), &_b); err != nil {
				return err
			}
			if _b.OrderUserID != h.UserID || _b.OrderID != h.EntID {
				continue
			}
			h.orderCommissionStatements[statement.UserID] = statement
		}
		offset += limit
	}
	return nil
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

func (h *orderHandler) getGoodCoin(ctx context.Context) error {
	coin, err := coinmwcli.GetCoin(ctx, h.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.goodCoin = coin
	return nil
}

func (h *orderHandler) getStableUSDCoin(ctx context.Context) error {
	if h.PaymentCoinTypeID != uuid.Nil.String() {
		return nil
	}
	coinName := "usdttrc20"
	if h.goodCoin.ENV == "test" {
		coinName = "tusdttrc20"
	}
	coin, err := coinmwcli.GetCoinOnly(ctx, &coinmwpb.Conds{
		Name: &basetypes.StringVal{Op: cruder.EQ, Value: coinName},
		ENV:  &basetypes.StringVal{Op: cruder.EQ, Value: h.goodCoin.ENV},
	})
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid stablecoin")
	}
	h.usdtTrc20Coin = coin
	h.PaymentCoinTypeID = h.usdtTrc20Coin.EntID
	h.CoinUSDCurrency = decimal.NewFromInt(1).String()
	return nil
}

func (h *orderHandler) calculateGoodValue() error {
	for _, order := range h.childOrders {
		amount, err := decimal.NewFromString(order.GoodValueUSD)
		if err != nil {
			return err
		}
		h.goodValueUSD = h.goodValueUSD.Add(amount)
	}

	amount, err := decimal.NewFromString(h.GoodValueUSD)
	if err != nil {
		return err
	}
	h.goodValueUSD = h.goodValueUSD.Add(amount)

	currency, err := decimal.NewFromString(h.CoinUSDCurrency)
	if err != nil {
		return err
	}
	if currency.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid currency")
	}
	h.goodValue = h.goodValueUSD.Div(currency)

	return nil
}

func (h *orderHandler) calculateAchievementStatements(ctx context.Context) error {
	hasCommission := false

	if h.Simulate {
		return nil
	}

	switch h.OrderType {
	case ordertypes.OrderType_Normal:
		hasCommission = true
	case ordertypes.OrderType_Offline:
	case ordertypes.OrderType_Airdrop:
		return nil
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

func (h *orderHandler) toAchievementStatementReqs() error {
	if h.Simulate {
		return nil
	}

	for _, statement := range h.statements {
		req := &achievementstatementmwpb.StatementReq{
			AppID:                  &statement.AppID,
			UserID:                 &statement.UserID,
			GoodID:                 &statement.GoodID,
			AppGoodID:              &statement.AppGoodID,
			OrderID:                &statement.OrderID,
			SelfOrder:              &statement.SelfOrder,
			PaymentID:              &statement.PaymentID,
			CoinTypeID:             &statement.CoinTypeID,
			PaymentCoinTypeID:      &statement.PaymentCoinTypeID,
			PaymentCoinUSDCurrency: &statement.PaymentCoinUSDCurrency,
			Units:                  &statement.Units,
			Amount:                 &statement.Amount,
			USDAmount:              &statement.USDAmount,
			Commission:             &statement.Commission,
			AppConfigID:            &statement.AppConfigID,
			CommissionConfigID:     &statement.CommissionConfigID,
			CommissionConfigType:   &statement.CommissionConfigType,
		}

		if statement.CommissionConfigType != inspiretypes.CommissionConfigType_LegacyCommissionConfig {
			orderCommissionStatement, ok := h.orderCommissionStatements[statement.UserID]
			if ok {
				var _b b
				if err := json.Unmarshal([]byte(orderCommissionStatement.IOExtra), &_b); err != nil {
					return err
				}
				commissionConfigType := inspiretypes.CommissionConfigType(inspiretypes.CommissionConfigType_value[_b.CommissionConfigType])
				req.AppConfigID = &_b.InspireAppConfigID
				req.CommissionConfigID = &_b.CommissionConfigID
				req.CommissionConfigType = &commissionConfigType
				req.Commission = &orderCommissionStatement.Amount
			} else {
				commission := "0"
				req.Commission = &commission
			}
		}

		if _, err := uuid.Parse(statement.DirectContributorID); err == nil {
			req.DirectContributorID = &statement.DirectContributorID
		}
		h.achievementStatements = append(h.achievementStatements, req)
	}
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"PaymentAmount", h.paymentAmount,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:                 h.Order,
		AchievementStatements: h.achievementStatements,
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
	if err = h.getGoodCoin(ctx); err != nil {
		return err
	}
	if err = h.getStableUSDCoin(ctx); err != nil {
		return err
	}
	if err = h.getOrdersCommissionStatements(ctx); err != nil {
		return err
	}
	if err = h.calculateGoodValue(); err != nil {
		return err
	}
	if err = h.calculateAchievementStatements(ctx); err != nil {
		return err
	}
	if err = h.toAchievementStatementReqs(); err != nil {
		return err
	}

	return nil
}
