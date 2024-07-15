package persistent

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	statementmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger/statement"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/bookkeeping/user/types"
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

func (p *handler) checkBenefitStatement(ctx context.Context, reward *types.OrderReward) (bool, error) {
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_MiningBenefit
	exist, err := statementmwcli.ExistStatementConds(
		ctx,
		&statementmwpb.Conds{
			AppID:     &basetypes.StringVal{Op: cruder.EQ, Value: reward.AppID},
			UserID:    &basetypes.StringVal{Op: cruder.EQ, Value: reward.UserID},
			IOType:    &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ioType)},
			IOSubType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ioSubType)},
		})
	if err != nil {
		return false, wlog.WrapError(err)
	}
	return exist, nil
}

func (p *handler) rewardFirstBenefit(ctx context.Context, good *types.PersistentGood) {
	for _, reward := range good.OrderRewards {
		existBenefit, err := p.checkBenefitStatement(ctx, reward)
		if err != nil {
			logger.Sugar().Errorw(
				"checkBenefitStatement",
				"AppID", reward.AppID,
				"UserID", reward.UserID,
				"Error", err,
			)
			continue
		}
		if existBenefit {
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

func (p *handler) withUpdateOrderBenefitState(dispose *dtmcli.SagaDispose, good *types.PersistentGood) {
	reqs := []*powerrentalordermwpb.PowerRentalOrderReq{}
	state := ordertypes.BenefitState_BenefitBookKept
	for _, order := range good.OrderRewards {
		reqs = append(reqs, &powerrentalordermwpb.PowerRentalOrderReq{
			OrderID:      &order.OrderID,
			BenefitState: &state,
		})
	}
	dispose.Add(
		ordersvcname.ServiceDomain,
		"order.middleware.powerrental.v1.Middleware/UpdatePowerRentalOrders",
		"",
		&powerrentalordermwpb.UpdatePowerRentalOrdersRequest{
			Infos: reqs,
		},
	)
}

func (p *handler) withCreateLedgerStatements(dispose *dtmcli.SagaDispose, good *types.PersistentGood) {
	reqs := []*statementmwpb.StatementReq{}

	rollback := true
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_MiningBenefit

	for _, reward := range good.OrderRewards {
		for _, coinReward := range reward.CoinRewards {
			reqs = append(reqs, &statementmwpb.StatementReq{
				EntID:      func() *string { s := uuid.NewString(); return &s }(),
				AppID:      &reward.AppID,
				UserID:     &reward.UserID,
				CoinTypeID: &coinReward.CoinTypeID,
				IOType:     &ioType,
				IOSubType:  &ioSubType,
				Amount:     &coinReward.Amount,
				IOExtra:    &reward.Extra,
				CreatedAt:  &good.LastRewardAt,
				Rollback:   &rollback,
			})
		}
	}

	if len(reqs) == 0 {
		return
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.statement.v2.Middleware/CreateStatements",
		"ledger.middleware.ledger.statement.v2.Middleware/DeleteStatements",
		&statementmwpb.CreateStatementsRequest{
			Infos: reqs,
		},
	)
}

func (p *handler) updateGood(ctx context.Context, good *types.PersistentGood) error {
	state := goodtypes.BenefitState_BenefitSimulateBookKeeping
	return powerrentalmwcli.UpdatePowerRental(ctx, &powerrentalmwpb.PowerRentalReq{
		ID:          &good.ID,
		RewardState: &state,
		RewardAt:    &good.LastRewardAt,
	})
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

	p.rewardFirstBenefit(ctx, _good)

	return nil
}
