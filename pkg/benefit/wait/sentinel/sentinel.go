package sentinel

import (
	"context"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
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

func (h *handler) scanGoods(ctx context.Context, state goodtypes.BenefitState, goodIDs map[string]struct{}, exec chan interface{}) error {
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
			if goodIDs != nil {
				if _, ok := goodIDs[good.ID]; !ok {
					continue
				}
			}
			cancelablefeed.CancelableFeed(ctx, good, exec)
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	if time.Now().Before(h.NextBenefitAt()) {
		return nil
	}
	h.CalculateNextBenefitAt()
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitWait, nil, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	goodIDs := map[string]struct{}{}
	if _cond, ok := cond.(*types.TriggerCond); ok {
		if _cond.GoodID != nil {
			goodIDs[*_cond.GoodID] = struct{}{}
		}
		if _cond.GoodIDs != nil {
			for _, goodID := range *_cond.GoodIDs {
				goodIDs[goodID] = struct{}{}
			}
		}
	}
	logger.Sugar().Infow(
		"TriggerScan",
		"GoodIDs", goodIDs,
	)
	if len(goodIDs) == 0 {
		return h.scanGoods(ctx, goodtypes.BenefitState_BenefitWait, nil, exec)
	}
	return h.scanGoods(ctx, goodtypes.BenefitState_BenefitWait, goodIDs, exec)
}

func (h *handler) ObjectID(ent interface{}) string {
	if good, ok := ent.(*types.PersistentGood); ok {
		return good.ID
	}
	return ent.(*goodmwpb.Good).ID
}
