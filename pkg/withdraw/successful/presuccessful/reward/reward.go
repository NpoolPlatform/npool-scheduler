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
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/presuccessful/types"
)

type handler struct{}

func NewReward() basereward.Rewarded {
	return &handler{}
}

func (p *handler) rewardWithdraw(_withdraw *types.PersistentWithdraw) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		req := &eventmwpb.CalcluateEventRewardsRequest{
			AppID:       _withdraw.AppID,
			UserID:      _withdraw.UserID,
			EventType:   basetypes.UsedFor_WithdrawalCompleted,
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
			"rewardWithdraw",
			"AppID", _withdraw.AppID,
			"UserID", _withdraw.UserID,
			"Error", err,
		)
	}
}

func (p *handler) Update(ctx context.Context, withdraw interface{}, notif, done chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	defer asyncfeed.AsyncFeed(ctx, _withdraw, done)

	p.rewardWithdraw(_withdraw)

	return nil
}
