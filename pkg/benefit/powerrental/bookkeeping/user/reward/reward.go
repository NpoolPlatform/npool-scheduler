package reward

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basereward "github.com/NpoolPlatform/npool-scheduler/pkg/base/reward"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/bookkeeping/user/types"
)

type handler struct{}

func NewReward() basereward.Rewarded {
	return &handler{}
}

func (p *handler) rewardFirstBenefit(good *types.PersistentGood) {
	for _, reward := range good.OrderRewards {
		if !reward.FirstBenefit {
			continue
		}
		if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
			req := &eventmwpb.CalcluateEventRewardsRequest{
				AppID:       reward.AppID,
				UserID:      reward.UserID,
				EventType:   basetypes.UsedFor_FirstBenefit,
				Consecutive: 1,
			}
			return publisher.Update(
				basetypes.MsgID_CalculateEventRewardReq.String(),
				nil,
				nil,
				nil,
				req,
			)
		}); err != nil {
			logger.Sugar().Errorw(
				"rewardFirstBenefit",
				"AppID", reward.AppID,
				"UserID", reward.UserID,
				"Error", err,
			)
		}
	}
}

func (p *handler) Update(ctx context.Context, good interface{}, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGood)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, done)

	p.rewardFirstBenefit(_good)

	return nil
}
