package persistent

import (
	"context"
	"fmt"
	"time"

	depositaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/finish/types"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
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

func (p *handler) Update(ctx context.Context, account interface{}, reward, notif, done chan interface{}) error {
	_account, ok := account.(*types.PersistentAccount)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	defer asyncfeed.AsyncFeed(ctx, _account, done)

	if err := accountlock.Lock(_account.AccountID); err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(_account.AccountID) //nolint
	}()

	scannableAt := uint32(time.Now().Unix() + timedef.SecondsPerHour)
	locked := false
	collectingID := uuid.Nil.String()
	if _, err := depositaccmwcli.UpdateAccount(ctx, &depositaccmwpb.AccountReq{
		ID:            &_account.ID,
		AppID:         &_account.AppID,
		UserID:        &_account.UserID,
		CoinTypeID:    &_account.CoinTypeID,
		AccountID:     &_account.AccountID,
		Locked:        &locked,
		CollectingTID: &collectingID,
		ScannableAt:   &scannableAt,
		Outcoming:     _account.CollectOutcoming,
	}); err != nil {
		return err
	}

	p.rewardDeposit(_account)

	return nil
}
