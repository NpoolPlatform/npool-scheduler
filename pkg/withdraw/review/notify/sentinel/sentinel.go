package sentinel

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	ledgerwithdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	ledgerwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"

	"github.com/google/uuid"
)

type handler struct {
	ID             string
	nextNotifyAt   uint32
	notifyInterval uint32
}

func NewSentinel() basesentinel.Scanner {
	_interval := timedef.SecondsPerHour
	if interval, err := time.ParseDuration(
		fmt.Sprintf("%vh", os.Getenv("ENV_WITHDRAW_REVIEW_NOTIFY_INTERVAL_HOURS"))); err == nil && math.Round(interval.Seconds()) > 0 {
		_interval = int(math.Round(interval.Seconds()))
	}
	return &handler{
		ID:             uuid.NewString(),
		nextNotifyAt:   uint32((int(time.Now().Unix()) + _interval) / _interval * _interval),
		notifyInterval: uint32(_interval),
	}
}

func (h *handler) scanWithdraws(ctx context.Context, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	withdraws := []*ledgerwithdrawmwpb.Withdraw{}

	for {
		_withdraws, _, err := ledgerwithdrawmwcli.GetWithdraws(ctx, &ledgerwithdrawmwpb.Conds{
			State: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ledgertypes.WithdrawState_Reviewing)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(_withdraws) == 0 {
			break
		}
		withdraws = append(withdraws, _withdraws...)
		offset += limit
	}
	if len(withdraws) > 0 {
		cancelablefeed.CancelableFeed(ctx, withdraws, exec)
	}
	return nil
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if uint32(time.Now().Unix()) < h.nextNotifyAt {
		return nil
	}
	if err := h.scanWithdraws(ctx, exec); err != nil {
		return err
	}
	h.nextNotifyAt = (uint32(time.Now().Unix()) + h.notifyInterval) / h.notifyInterval * h.notifyInterval
	return nil
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanWithdraws(ctx, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	return uuid.Nil.String()
}
