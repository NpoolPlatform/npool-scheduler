package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	achievementstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/achievement/types"

	"github.com/google/uuid"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent       chan interface{}
	notif            chan interface{}
	statements       []*achievementstatementmwpb.Statement
	ledgerStatements []*ledgerstatementmwpb.StatementReq
}

func (h *orderHandler) getOrderAchievement(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		statements, _, err := achievementstatementmwcli.GetStatements(ctx, &achievementstatementmwpb.Conds{
			OrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(statements) == 0 {
			return nil
		}
		h.statements = append(h.statements, statements...)
		offset += limit
	}
}

func (h *orderHandler) toLedgerStatements() {
	ioType := ledgertypes.IOType_Outcoming
	ioSubType := ledgertypes.IOSubType_CommissionRevoke
	for _, statement := range h.statements {
		ioExtra := fmt.Sprintf(
			`{"AppID":"%v","UserID":"%v","ArchivementStatementID":"%v","Amount":"%v","Date":"%v","CancelOrder":true}`,
			statement.AppID,
			statement.UserID,
			statement.ID,
			statement.Commission,
			time.Now(),
		)
		id := uuid.NewString()
		h.ledgerStatements = append(h.ledgerStatements, &ledgerstatementmwpb.StatementReq{
			ID:         &id,
			AppID:      &statement.AppID,
			UserID:     &statement.UserID,
			CoinTypeID: &statement.PaymentCoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &statement.Commission,
			IOExtra:    &ioExtra,
		})
	}
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
		cancelablefeed.CancelableFeed(ctx, persistentOrder, h.persistent)
	} else {
		cancelablefeed.CancelableFeed(ctx, persistentOrder, h.notif)
	}
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if err = h.getOrderAchievement(ctx); err != nil {
		return err
	}
	h.toLedgerStatements()

	return nil
}
