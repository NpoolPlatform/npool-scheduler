package notif

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	basenotif "github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/types"

	"github.com/google/uuid"
)

type handler struct{}

func NewNotif() basenotif.Notify {
	return &handler{}
}

func (p *handler) notifyWithdrawReview(notify *types.PersistentWithdrawReviewNotify) error {
	return pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		return publisher.Update(
			basetypes.MsgID_WithdrawReviewNotify.String(),
			nil,
			nil,
			nil,
			notify.AppWithdraws,
		)
	})
}

func (p *handler) Notify(ctx context.Context, notify interface{}, retry chan interface{}) error {
	_notify, ok := notify.(*types.PersistentWithdrawReviewNotify)
	if !ok {
		return fmt.Errorf("invalid notify")
	}
	if err := p.notifyWithdrawReview(_notify); err != nil {
		retry1.Retry(uuid.Nil.String(), _notify, retry)
		return err
	}
	return nil
}
