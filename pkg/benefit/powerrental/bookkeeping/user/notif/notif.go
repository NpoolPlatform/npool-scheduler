package notif

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	basenotif "github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/bookkeeping/user/types"
)

type handler struct{}

func NewNotif() basenotif.Notify {
	return &handler{}
}

func (p *handler) notifyGoodBenefit(good *types.PersistentGood) error {
	return pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		now := uint32(time.Now().Unix())
		req := &notifbenefitmwpb.GoodBenefitReq{
			GoodID:      &good.GoodID,
			GoodType:    &good.GoodType,
			GoodName:    &good.Name,
			State:       &good.BenefitResult,
			BenefitDate: &now,
		}
		for _, reward := range good.Rewards {
			req.CoinTypeID = &reward.CoinTypeID
			req.Message = func() *string {
				if good.Error == nil {
					return nil
				}
				s := wlog.Unwrap(good.Error).Error()
				return &s
			}()
			if err := publisher.Update(
				basetypes.MsgID_CreateGoodBenefitReq.String(),
				nil,
				nil,
				nil,
				req,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (p *handler) Notify(ctx context.Context, good interface{}, retry chan interface{}) error {
	_good, ok := good.(*types.PersistentGood)
	if !ok {
		return fmt.Errorf("invalid good")
	}
	if err := p.notifyGoodBenefit(_good); err != nil {
		retry1.Retry(_good.EntID, _good, retry)
		return err
	}
	return nil
}
