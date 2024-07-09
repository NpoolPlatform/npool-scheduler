package notif

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodcoinrewardmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/coin/reward"
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	basenotif "github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/done/types"
)

type handler struct{}

func NewNotif() basenotif.Notify {
	return &handler{}
}

func (p *handler) notifyGoodBenefit(good *types.PersistentGood) error {
	return pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		now := uint32(time.Now().Unix())
		result := basetypes.Result_Success

		for _, reward := range good.CoinNextRewards {
			_reward := func() *goodcoinrewardmwpb.RewardInfo {
				for _, r := range good.Rewards {
					if r.CoinTypeID == reward.CoinTypeID {
						return r
					}
				}
				return nil
			}()

			if _reward == nil {
				return fmt.Errorf("invalid reward")
			}

			req := &notifbenefitmwpb.GoodBenefitReq{
				GoodID:      &good.GoodID,
				GoodType:    &good.GoodType,
				GoodName:    &good.Name,
				Amount:      &_reward.LastRewardAmount,
				TxID:        &_reward.RewardTID,
				State:       &result,
				BenefitDate: &now,
				CoinTypeID:  &reward.CoinTypeID,
				Message:     &reward.BenefitMessage,
			}

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
