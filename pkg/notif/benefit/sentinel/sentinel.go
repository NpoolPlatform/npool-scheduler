package sentinel

import (
	"context"

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
	ID string
}

func NewSentinel() basesentinel.Scanner {
	return &handler{
		ID: uuid.NewString(),
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
		if len(benefits) == 0 {
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
	return h.scanGoodBenefits(ctx, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	return h.ID
}
