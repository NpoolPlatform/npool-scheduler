package sentinel

import (
	"context"
	"time"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	common "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct {
	*common.Handler
}

func NewSentinel() basesentinel.Scanner {
	h := &handler{
		Handler: common.NewHandler(),
	}
	return h
}

func (h *handler) feedGood(ctx context.Context, good *goodmwpb.Good, exec chan interface{}) error {
	if good.RewardState == goodtypes.BenefitState_BenefitWait {
		state := goodtypes.BenefitState_BenefitCheckWait
		if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
			ID:          &good.ID,
			RewardState: &state,
		}); err != nil {
			return err
		}
	}
	cancelablefeed.CancelableFeed(ctx, good, exec)
	return nil
}

func (h *handler) scanGoods(ctx context.Context, state goodtypes.BenefitState, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		goods, _, err := goodmwcli.GetGoods(ctx, &goodmwpb.Conds{
			RewardState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			return nil
		}

		for _, good := range goods {
			if err := h.feedGood(ctx, good, exec); err != nil {
				return err
			}
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if time.Now().Before(h.NextBenefitAt()) {
		return nil
	}
	h.CalculateNextBenefitAt()
	if err := h.scanGoods(ctx, goodtypes.BenefitState_BenefitWait, exec); err != nil {
		return err
	}
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitCheckWait, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitCheckWait, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitWait, exec)
}

func (h *handler) ObjectID(ent interface{}) string {
	if good, ok := ent.(*types.PersistentGood); ok {
		return good.ID
	}
	return ent.(*goodmwpb.Good).ID
}
