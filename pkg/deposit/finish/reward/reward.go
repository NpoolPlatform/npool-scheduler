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
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/finish/types"
)

type handler struct{}

func NewReward() basereward.Rewarded {
	return &handler{}
}

func (p *handler) rewardDeposit(_account *types.PersistentAccount) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		req := &eventmwpb.CalcluateEventRewardsRequest{
			AppID:       _account.AppID,
			UserID:      _account.UserID,
			EventType:   basetypes.UsedFor_DepositReceived,
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
			"rewardDeposit",
			"AppID", _account.AppID,
			"UserID", _account.UserID,
			"Error", err,
		)
	}
}

func (p *handler) Update(ctx context.Context, account interface{}, notif, done chan interface{}) error {
	_account, ok := account.(*types.PersistentAccount)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	defer asyncfeed.AsyncFeed(ctx, _account, done)

	p.rewardDeposit(_account)

	return nil
}
