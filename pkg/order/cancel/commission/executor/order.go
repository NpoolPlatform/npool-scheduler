package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	achievementstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	achievementstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	orderlockmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order/orderlock"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/cancel/commission/types"
	orderlockmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order/orderlock"

	"github.com/google/uuid"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent       chan interface{}
	notif            chan interface{}
	done             chan interface{}
	statements       []*achievementstatementmwpb.Statement
	ledgerStatements []*ledgerstatementmwpb.StatementReq
	commissionLocks  []*orderlockmwpb.OrderLock
}

func (h *orderHandler) getOrderCommissionLock(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		locks, _, err := orderlockmwcli.GetOrderLocks(ctx, &orderlockmwpb.Conds{
			OrderID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
			LockType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.OrderLockType_LockCommission)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(locks) == 0 {
			break
		}
		h.commissionLocks = append(h.commissionLocks, locks...)
		offset += limit
	}
	return nil
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
		CommissionLocks:  map[string]*orderlockmwpb.OrderLock{},
	}
	for _, lock := range h.commissionLocks {
		persistentOrder.CommissionLocks[lock.UserID] = lock
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

	if err = h.getOrderCommissionLock(ctx); err != nil {
		return err
	}
	if err = h.getOrderAchievement(ctx); err != nil {
		return err
	}
	h.toLedgerStatements()

	return nil
}
