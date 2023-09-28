package sentinel

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	notifbenefitmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/goodbenefit"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"

	"github.com/google/uuid"
)

type handler struct {
	ID              string
	nextBenefitAt   uint32
	benefitInterval uint32
}

func NewSentinel() basesentinel.Scanner {
	_interval := timedef.SecondsPerHour
	if interval, err := time.ParseDuration(
		fmt.Sprintf("%vm", os.Getenv("ENV_BENEFIT_NOTIFY_INTERVAL_MINS"))); err == nil && math.Round(interval.Seconds()) > 0 {
		_interval = int(math.Round(interval.Seconds()))
	}
	return &handler{
		ID:              uuid.NewString(),
		nextBenefitAt:   uint32((int(time.Now().Unix()) + _interval) / _interval * _interval),
		benefitInterval: uint32(_interval),
	}
}

func (h *handler) scanGoodBenefits(ctx context.Context, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	benefits := []*notifbenefitmwpb.GoodBenefit{}

	for {
		_benefits, _, err := notifbenefitmwcli.GetGoodBenefits(ctx, &notifbenefitmwpb.Conds{
			Generated: &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(_benefits) == 0 {
			break
		}
		benefits = append(benefits, _benefits...)
		offset += limit
	}
	if len(benefits) > 0 {
		cancelablefeed.CancelableFeed(ctx, benefits, exec)
	}
	return nil
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if uint32(time.Now().Unix()) < h.nextBenefitAt {
		return nil
	}
	if err := h.scanGoodBenefits(ctx, exec); err != nil {
		return err
	}
	h.nextBenefitAt = (uint32(time.Now().Unix()) + h.benefitInterval) / h.benefitInterval * h.benefitInterval
	return nil
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanGoodBenefits(ctx, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	return h.ID
}
