package reward

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basereward "github.com/NpoolPlatform/npool-scheduler/pkg/base/reward"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/paid/stock/types"
)

type handler struct{}

func NewReward() basereward.Rewarder {
	return &handler{}
}

func (p *handler) rewardOrderCompleted(order *types.PersistentOrder) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		req := &eventmwpb.CalcluateEventRewardsRequest{
			AppID:       order.AppID,
			UserID:      order.UserID,
			EventType:   basetypes.UsedFor_OrderCompleted,
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
			"rewardOrderCompleted",
			"AppID", order.AppID,
			"UserID", order.UserID,
			"Error", err,
		)
	}
}

func (p *handler) rewardFirstOrderCompleted(order *types.PersistentOrder) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		req := &eventmwpb.CalcluateEventRewardsRequest{
			AppID:       order.AppID,
			UserID:      order.UserID,
			EventType:   basetypes.UsedFor_FirstOrderCompleted,
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
			"rewardFirstOrderCompleted",
			"AppID", order.AppID,
			"UserID", order.UserID,
			"Error", err,
		)
	}
}

func (p *handler) Update(ctx context.Context, order interface{}, notif, done chan interface{}) error {
	_order, ok := order.(*types.PersistentOrder)
	if !ok {
		return fmt.Errorf("invalid order")
	}

	defer asyncfeed.AsyncFeed(ctx, _order, done)

	if _order.OrderType == ordertypes.OrderType_Normal {
		p.rewardFirstOrderCompleted(_order)
		p.rewardOrderCompleted(_order)
	}

	return nil
}
