package sentinel

import (
	"context"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	h := &handler{}
	return h
}

func (h *handler) feedGood(ctx context.Context, good *goodmwpb.Good, exec chan interface{}) error {
	if good.RewardState == goodtypes.BenefitState_BenefitBookKeeping {
		state := goodtypes.BenefitState_BenefitCheckBookKeeping
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
			if good.ID != "55e2b420-fc00-479f-9159-6fce48df83f8" {
				continue
			}
			if err := h.feedGood(ctx, good, exec); err != nil {
				return err
			}
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if err := h.scanGoods(ctx, goodtypes.BenefitState_BenefitCheckBookKeeping, exec); err != nil {
		return err
	}
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitBookKeeping, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitCheckBookKeeping, exec)
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitBookKeeping, exec)
}

func (h *handler) ObjectID(ent interface{}) string {
	if good, ok := ent.(*types.PersistentGood); ok {
		return good.ID
	}
	return ent.(*goodmwpb.Good).ID
}
