package persistent

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	withdrawmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/withdraw"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/presuccessful/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
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

func (p *handler) Update(ctx context.Context, withdraw interface{}, reward, notif, done chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	defer asyncfeed.AsyncFeed(ctx, _withdraw, done)

	state := ledgertypes.WithdrawState_SpendSuccessfulBalance
	if _, err := withdrawmwcli.UpdateWithdraw(ctx, &withdrawmwpb.WithdrawReq{
		ID:    &_withdraw.ID,
		State: &state,
	}); err != nil {
		return err
	}

	p.rewardWithdraw(_withdraw)

	return nil
}
