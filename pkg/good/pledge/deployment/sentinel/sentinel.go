package sentinel

import (
	"context"

	"github.com/NpoolPlatform/build-chain/pkg/constant"
	goodpledgemwcli "github.com/NpoolPlatform/good-middleware/pkg/client/pledge"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodbasepb "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	v1 "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodpledgemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/pledge"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/pledge/wait/types"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) scanPledges(ctx context.Context, goodType goodbasepb.GoodType, contractState goodbasepb.ContractState, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		goods, _, err := goodpledgemwcli.GetPledges(ctx, &goodpledgemwpb.Conds{
			GoodType: &v1.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(goodType),
			},
			ContractState: &v1.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(contractState),
			},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			return nil
		}
		for _, good := range goods {
			cancelablefeed.CancelableFeed(ctx, good, exec)
		}
		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	return h.scanPledges(ctx,
		goodbasepb.GoodType_Pledge,
		goodbasepb.ContractState_ContractInDeployment,
		exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	if tx, ok := ent.(*types.PersistentGoodPledge); ok {
		return tx.EntID
	}
	return ent.(*goodpledgemwpb.Pledge).EntID
}
