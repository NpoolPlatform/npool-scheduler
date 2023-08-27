package sentinel

import (
	"context"
	"fmt"
	"os"
	"time"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct {
	benefitInterval  time.Duration
	nextBenefitAt    time.Time
	benefitTimestamp uint32
}

func NewSentinel() basesentinel.Scanner {
	h := &handler{
		benefitInterval: 24 * time.Hour,
	}
	h.prepareInterval()
	return h
}

func (h *handler) prepareInterval() {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS"))); err == nil {
		h.benefitInterval = duration
	}
	h.calculateNextBenefitAt()
}

func (h *handler) calculateNextBenefitAt() {
	now := time.Now()
	nowSec := now.Unix()
	benefitSeconds := int64(h.benefitInterval.Seconds())
	nextSec := (nowSec + benefitSeconds) / benefitSeconds * benefitSeconds
	h.nextBenefitAt = now.Add(time.Duration(nextSec-nowSec) * time.Second)
	h.benefitTimestamp = h.benefitTimestampAt(uint32(time.Now().Unix()))
}

func (h *handler) benefitTimestampAt(timestamp uint32) uint32 {
	intervalFloat := h.benefitInterval.Seconds()
	intervalUint := uint32(intervalFloat)
	return timestamp / intervalUint * intervalUint
}

func (h *handler) scanGoods(ctx context.Context, rescan bool, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		conds := &goodmwpb.Conds{
			RewardState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(goodtypes.BenefitState_BenefitWait)},
		}
		if rescan {
			conds.RewardAt = &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(h.benefitTimestamp)}
		} else {
			conds.RewardAt = &basetypes.Uint32Val{Op: cruder.NEQ, Value: uint32(h.benefitTimestamp)}
		}
		goods, _, err := goodmwcli.GetGoods(ctx, conds, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			return nil
		}

		for _, good := range goods {
			exec <- good
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if time.Now().Before(h.nextBenefitAt) {
		return nil
	}
	h.calculateNextBenefitAt()
	return h.scanGoods(ctx, false, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanGoods(ctx, true, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}
