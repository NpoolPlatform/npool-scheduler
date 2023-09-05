package persistent

import (
	"context"
	"fmt"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	tmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	notifbenefitmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/goodbenefit"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/benefit/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, benefit interface{}, retry, notif, done chan interface{}) error {
	_benefit, ok := benefit.(*types.PersistentGoodBenefit)
	if !ok {
		return fmt.Errorf("invalid benefit")
	}

	defer asyncfeed.AsyncFeed(ctx, _benefit, done)

	if !_benefit.Generated {
		for _, content := range _benefit.NotifContents {
			if _, err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
				AppID:     content.AppID,
				EventType: basetypes.UsedFor_GoodBenefit1,
				NotifType: basetypes.NotifType_NotifMulticast,
				Vars: &tmplmwpb.TemplateVars{
					Message: &content.Content,
				},
			}); err != nil {
				retry1.Retry(ctx, _benefit, retry)
				return err
			}
		}
	}

	_benefit.Generated = true

	for _, benefit := range _benefit.Benefits {
		if _, err := notifbenefitmwcli.UpdateGoodBenefit(ctx, &notifbenefitmwpb.GoodBenefitReq{
			ID:        &benefit.ID,
			Generated: &_benefit.Generated,
		}); err != nil {
			retry1.Retry(ctx, _benefit, retry)
			return err
		}
	}

	return nil
}
