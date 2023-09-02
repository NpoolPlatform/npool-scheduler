package executor

import (
	"context"
	"fmt"

	calculatemwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/calculate"
	inspiretypes "github.com/NpoolPlatform/message/npool/basetypes/inspire/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	calculatemwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/calculate"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/payment/stock/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent       chan interface{}
	paymentAmount    decimal.Decimal
	statements       []*achievementstatementmwpb.Statement
	ledgerStatements []*ledgerstatementmwpb.StatementReq
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
	h.statements = statements
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
			`{"PaymentID":"%v","OrderID":"%v","DirectContributorID":"%v","OrderUserID":"%v"}`,
			h.PaymentID,
			h.ID,
			statement.DirectContributorID,
			h.UserID,
		)
		h.ledgerStatements = append(h.ledgerStatements, &ledgerstatementmwpb.StatementReq{
			AppID:      &h.AppID,
			UserID:     &statement.UserID,
			CoinTypeID: &h.PaymentCoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &statement.Commission,
			IOExtra:    &ioExtra,
		})
	}
	return nil
}

func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		return
	}

	persistentOrder := &types.PersistentOrder{
		Order:            h.Order,
		LedgerStatements: h.ledgerStatements,
	}
	asyncfeed.AsyncFeed(persistentOrder, h.persistent)
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
	if err = h.calculateLedgerStatements(); err != nil {
		return err
	}

	return nil
}
