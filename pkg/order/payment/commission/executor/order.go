package executor

import (
	"context"

	calculatemwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/calculate"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	calculatemwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/calculate"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/commission/types"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent            chan interface{}
	achievementStatements []*achievementstatementmwpb.StatementReq
	paymentAmount         decimal.Decimal
}

func (h *orderHandler) calculateAchievementStatements(ctx context.Context) error {
	hasCommission := false

	switch h.OrderType {
	case ordertypes.OrderType_Normal:
		hasCommission = true
	case ordertypes.OrderType_Offline:
	case ordertypes.OrderType_Airdrop:
	}

	statements, err := calculatemwcli.Calculate(ctx, &calculatemwpb.CalculateRequest{
		AppID:                  h.AppID,
		UserID:                 h.UserID,
		GoodID:                 h.GoodID,
		OrderID:                h.ID,
		PaymentID:              h.PaymentID,
		CoinTypeID:             h.CoinTypeID,
		PaymentCoinTypeID:      h.PaymentCoinTypeID,
		PaymentCoinUSDCurrency: h.CoinUSDCurrency,
		Units:                  h.Units,
		PaymentAmount:          h.PaymentAmount,
		GoodValue:              h.GoodValue,
		GoodValueUSD:           h.GoodValueUSD,
		SettleType:             inspiretypes.SettleType_GoodOrderPayment,
		HasCommission:          hasCommission,
		OrderCreatedAt:         h.CreatedAt,
	})
	if err != nil {
		return err
	}

	for _, statement := range statements {
		req := &achievementstatementmwpb.StatementReq{
			AppID:                  &statement.AppID,
			UserID:                 &statement.UserID,
			GoodID:                 &statement.GoodID,
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
		}
		if _, err := uuid.Parse(statement.DirectContributorID); err == nil {
			req.DirectContributorID = &statement.DirectContributorID
		}
		h.achievementStatements = append(h.achievementStatements, req)
	}

	return nil
}

func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		return
	}

	persistentOrder := &types.PersistentOrder{
		Order:                 h.Order,
		AchievementStatements: h.achievementStatements,
	}
	h.persistent <- persistentOrder
}

func (h *orderHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if h.paymentAmount, err = decimal.NewFromString(h.PaymentAmount); err != nil {
		return err
	}
	if err = h.calculateAchievementStatements(ctx); err != nil {
		return err
	}

	return nil
}