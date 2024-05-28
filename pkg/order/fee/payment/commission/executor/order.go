package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	orderpaymentstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement/order/payment"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	orderpaymentstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement/order/payment"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/payment/commission/types"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent        chan interface{}
	notif             chan interface{}
	done              chan interface{}
	paymentStatements []*orderpaymentstatementmwpb.Statement
	ledgerStatements  []*ledgerstatementmwpb.StatementReq
}

func (h *orderHandler) getOrderPaymentStatements(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		statements, _, err := orderpaymentstatementmwcli.GetStatements(ctx, &orderpaymentstatementmwpb.Conds{
			OrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(statements) == 0 {
			return nil
		}
		h.paymentStatements = append(h.paymentStatements, statements...)
		offset += limit
	}
}

func (h *orderHandler) constructLedgerStatements() error {
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_Commission

	for _, statement := range h.paymentStatements {
		amount, err := decimal.NewFromString(statement.CommissionAmount)
		if err != nil {
			return err
		}
		if amount.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}
		ioExtra := fmt.Sprintf(
			`{"PaymentID":"%v","OrderID":"%v","OrderUserID":"%v","InspireAppConfigID":"%v","CommissionConfigID":"%v","CommissionConfigType":"%v"}`,
			h.PaymentID,
			h.EntID,
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
			Amount:     &statement.CommissionAmount,
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
			"PowerRentalOrder", h.PowerRentalOrder,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		PowerRentalOrder: h.PowerRentalOrder,
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

	if err := h.getOrderPaymentStatements(ctx); err != nil {
		return err
	}
	if err = h.constructLedgerStatements(); err != nil {
		return err
	}

	return nil
}
