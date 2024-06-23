package executor

import (
	"context"
	"encoding/json"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	achievementorderpaymentstatementmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/achievement/statement/order/payment"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	achievementorderpaymentstatementmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/achievement/statement/order/payment"
	orderlockmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order/lock"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/cancel/commission/types"
	orderlockmwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order/lock"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder
	persistent        chan interface{}
	notif             chan interface{}
	done              chan interface{}
	statements        []*achievementorderpaymentstatementmwpb.Statement
	commissionLocks   map[string]*orderlockmwpb.OrderLock
	commissionRevokes map[string]*types.CommissionRevoke
}

func (h *orderHandler) getOrderCommissionLock(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		locks, _, err := orderlockmwcli.GetOrderLocks(ctx, &orderlockmwpb.Conds{
			OrderID:  &basetypes.StringVal{Op: cruder.EQ, Value: h.OrderID},
			LockType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.OrderLockType_LockCommission)},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(locks) == 0 {
			break
		}
		for _, lock := range locks {
			h.commissionLocks[lock.UserID] = lock
		}
		offset += limit
	}
	return nil
}

func (h *orderHandler) getOrderAchievement(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		statements, _, err := achievementorderpaymentstatementmwcli.GetStatements(ctx, &achievementorderpaymentstatementmwpb.Conds{
			OrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.OrderID},
		}, offset, limit)
		if err != nil {
			return wlog.WrapError(err)
		}
		if len(statements) == 0 {
			return nil
		}
		h.statements = append(h.statements, statements...)
		offset += limit
	}
}

func (h *orderHandler) constructCommissionRevoke() error {
	h.commissionRevokes = map[string]*types.CommissionRevoke{}

	for _, statement := range h.statements {
		amount, err := decimal.NewFromString(statement.CommissionAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		if amount.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}
		extra := struct {
			AppID                   string          `json:"AppID"`
			UserID                  string          `json:"UserID"`
			AchievementStatementIDs []string        `json:"AchievementStatementIDs"`
			Amount                  decimal.Decimal `json:"Amount"`
			CancelOrder             bool            `json:"CancelOrder"`
		}{
			CancelOrder: true,
		}
		revoke, ok := h.commissionRevokes[statement.UserID]
		if !ok {
			lock, ok := h.commissionLocks[statement.UserID]
			if !ok {
				return wlog.Errorf("invalid commission lock")
			}
			revoke = &types.CommissionRevoke{
				LockID: lock.EntID,
			}
			extra.AppID = statement.AppID
			extra.UserID = statement.UserID
			extra.AchievementStatementIDs = []string{statement.EntID}
			extra.Amount = amount
		} else {
			if err := json.Unmarshal([]byte(revoke.IOExtra), &extra); err != nil {
				return wlog.WrapError(err)
			}
			extra.AchievementStatementIDs = append(extra.AchievementStatementIDs, statement.EntID)
			extra.Amount = extra.Amount.Add(amount)
		}
		_extra, err := json.Marshal(&extra)
		if err != nil {
			return wlog.WrapError(err)
		}
		revoke.IOExtra = string(_extra)
		revoke.StatementIDs = append(revoke.StatementIDs, uuid.NewString())
		h.commissionRevokes[statement.UserID] = revoke
	}
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil || true {
		logger.Sugar().Errorw(
			"final",
			"Order", h.PowerRentalOrder,
			"CommissionStatements", h.statements,
			"CommissionLocks", h.commissionLocks,
			"Error", *err,
		)
	}

	persistentOrder := &types.PersistentPowerRentalOrder{
		PowerRentalOrder: h.PowerRentalOrder,
		CommissionRevokes: func() (revokes []*types.CommissionRevoke) {
			for _, revoke := range h.commissionRevokes {
				revokes = append(revokes, revoke)
			}
			return
		}(),
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
		return wlog.WrapError(err)
	}
	if err = h.getOrderAchievement(ctx); err != nil {
		return wlog.WrapError(err)
	}
	if err = h.constructCommissionRevoke(); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
