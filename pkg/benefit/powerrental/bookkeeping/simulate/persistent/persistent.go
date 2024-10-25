package persistent

import (
	"context"
	"fmt"

	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	simstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/simulate/ledger/statement"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/bookkeeping/simulate/types"
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
	simReqs := []*simstatementmwpb.StatementReq{}
	realReqs := []*statementmwpb.StatementReq{}

	rollback := true
	ioType := ledgertypes.IOType_Incoming

	for _, reward := range good.OrderRewards {
		for _, coinReward := range reward.CoinRewards {
			simReqs = append(simReqs, &simstatementmwpb.StatementReq{
				EntID:      func() *string { s := uuid.NewString(); return &s }(),
				AppID:      &reward.AppID,
				UserID:     &reward.UserID,
				CoinTypeID: &coinReward.CoinTypeID,
				IOType:     &ioType,
				IOSubType:  func() *ledgertypes.IOSubType { e := ledgertypes.IOSubType_MiningBenefit; return &e }(),
				Amount:     &coinReward.Amount,
				IOExtra:    &reward.Extra,
				CreatedAt:  &good.LastRewardAt,
				Rollback:   &rollback,
				SendCoupon: &coinReward.SendCoupon,
				Cashable:   &coinReward.Cashable,
			})
			if !coinReward.Cashable {
				continue
			}
			realReqs = append(realReqs, &statementmwpb.StatementReq{
				EntID:      func() *string { s := uuid.NewString(); return &s }(),
				AppID:      &reward.AppID,
				UserID:     &reward.UserID,
				CoinTypeID: &coinReward.CoinTypeID,
				IOType:     &ioType,
				IOSubType:  func() *ledgertypes.IOSubType { e := ledgertypes.IOSubType_SimulateMiningBenefit; return &e }(),
				Amount:     &coinReward.Amount,
				IOExtra:    &reward.Extra,
				CreatedAt:  &good.LastRewardAt,
				Rollback:   &rollback,
			})
		}
	}

	if len(simReqs) > 0 {
		dispose.Add(
			ledgersvcname.ServiceDomain,
			"ledger.middleware.simulate.ledger.statement.v2.Middleware/CreateStatements",
			"ledger.middleware.simulate.ledger.statement.v2.Middleware/DeleteStatements",
			&simstatementmwpb.CreateStatementsRequest{
				Infos: simReqs,
			},
		)
	}

	if len(realReqs) > 0 {
		dispose.Add(
			ledgersvcname.ServiceDomain,
			"ledger.middleware.ledger.statement.v2.Middleware/CreateStatements",
			"ledger.middleware.ledger.statement.v2.Middleware/DeleteStatements",
			&statementmwpb.CreateStatementsRequest{
				Infos: realReqs,
			},
		)
	}
}

func (p *handler) updateGood(ctx context.Context, good *types.PersistentGood) error {
	state := goodtypes.BenefitState_BenefitDone
	return powerrentalmwcli.UpdatePowerRental(ctx, &powerrentalmwpb.PowerRentalReq{
		ID:          &good.ID,
		RewardState: &state,
	})
}

func (p *handler) Update(ctx context.Context, good interface{}, reward, notif, done chan interface{}) error {
	_good, ok := good.(*types.PersistentGood)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	defer asyncfeed.AsyncFeed(ctx, _good, reward)

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

	return nil
}
