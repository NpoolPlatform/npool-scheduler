package persistent

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	simstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/simulate/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/simulate/types"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	ordersvcname "github.com/NpoolPlatform/order-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateOrderBenefitState(dispose *dtmcli.SagaDispose, good *types.PersistentGood) {
	reqs := []*ordermwpb.OrderReq{}
	state := ordertypes.BenefitState_BenefitBookKept
	for _, order := range good.OrderRewards {
		reqs = append(reqs, &ordermwpb.OrderReq{
			ID:           &order.OrderID,
			BenefitState: &state,
		})
	}
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.order1.v1.Middleware/UpdateOrders",
		"",
		&ordermwpb.UpdateOrdersRequest{
			Infos: reqs,
		},
	)
}

func (p *handler) withCreateLedgerStatements(dispose *dtmcli.SagaDispose, good *types.PersistentGood) {
	reqs := []*simstatementmwpb.StatementReq{}

	rollback := true
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_MiningBenefit

	for _, reward := range good.OrderRewards {
		id := uuid.NewString()
		reqs = append(reqs, &simstatementmwpb.StatementReq{
			EntID:      &id,
			AppID:      &reward.AppID,
			UserID:     &reward.UserID,
			CoinTypeID: &good.CoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &reward.Amount,
			IOExtra:    &reward.Extra,
			CreatedAt:  &good.LastRewardAt,
			Rollback:   &rollback,
			SendCoupon: &reward.SendCoupon,
			Cashable:   &reward.Cashable,
		})
	}

	if len(reqs) > 0 {
		dispose.Add(
			ledgersvcname.ServiceDomain,
			"ledger.middleware.simulate.ledger.statement.v2.Middleware/CreateStatements",
			"ledger.middleware.simulate.ledger.statement.v2.Middleware/DeleteStatements",
			&simstatementmwpb.CreateStatementsRequest{
				Infos: reqs,
			},
		)
	}
}

func (p *handler) updateGood(ctx context.Context, good *types.PersistentGood) error {
	state := goodtypes.BenefitState_BenefitDone
	if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:          &good.ID,
		RewardState: &state,
	}); err != nil {
		return err
	}
	return nil
}

func (p *handler) rewardProfit(good *types.PersistentGood) {
	for _, reward := range good.OrderRewards {
		if !reward.SendCoupon {
			continue
		}
		if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
			req := &eventmwpb.RewardEventRequest{
				AppID:       reward.AppID,
				UserID:      reward.UserID,
				EventType:   basetypes.UsedFor_SimulateOrderProfit,
				Consecutive: 1,
			}
			return publisher.Update(
				basetypes.MsgID_RewardEventReq.String(),
				nil,
				nil,
				nil,
				req,
			)
		}); err != nil {
			logger.Sugar().Errorw(
				"rewardSimulateOrderProfit",
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

	if len(_good.OrderRewards) == 0 {
		if err := p.updateGood(ctx, _good); err != nil {
			return err
		}
		return nil
	}

	const timeoutSeconds = 60
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})
	p.withCreateLedgerStatements(sagaDispose, _good)
	p.withUpdateOrderBenefitState(sagaDispose, _good)
	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	p.rewardProfit(_good)

	return nil
}
