package notif

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	basenotif "github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/successful/spendbalance/types"
)

type handler struct{}

func NewNotif() basenotif.Notify {
	return &handler{}
}

func (p *handler) notifyWithdraw(withdraw *types.PersistentWithdraw) error {
	return pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		return publisher.Update(
			basetypes.MsgID_WithdrawSuccessReq.String(),
			nil,
			nil,
			nil,
			withdraw.Withdraw,
		)
	})
}

func (p *handler) Notify(ctx context.Context, withdraw interface{}, retry chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}
	if err := p.notifyWithdraw(_withdraw); err != nil {
		retry1.Retry(_withdraw.EntID, _withdraw, retry)
		return err
	}
	return nil
}
