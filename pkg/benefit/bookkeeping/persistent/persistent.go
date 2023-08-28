package persistent

import (
	"context"
	"fmt"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodstmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/good/ledger/statement"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	goodstmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/good/ledger/statement"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/types"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withCreateGoodLedgerStatement(dispose *dtmcli.SagaDispose, good *types.PersistentGood) {
	id := uuid.NewString()
	req := &goodstmwpb.GoodStatementReq{
		ID:                        &id,
		GoodID:                    &good.ID,
		CoinTypeID:                &good.CoinTypeID,
		TotalAmount:               &good.TotalRewardAmount,
		UnsoldAmount:              &good.UnsoldRewardAmount,
		TechniqueServiceFeeAmount: &good.TechniqueFeeAmount,
		BenefitDate:               &good.LastRewardAt,
	}

	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.good.ledger.statement.v2.Middleware/CreateGoodStatement",
		"ledger.middleware.good.ledger.statement.v2.Middleware/DeleteGoodStatement",
		&goodstmwpb.CreateGoodStatementRequest{
			Info: req,
		},
	)
}

func (p *handler) withCreateLedgerStatements(dispose *dtmcli.SagaDispose, good *types.PersistentGood) {
	reqs := []*statementmwpb.StatementReq{}
	for _, reward := range good.OrderRewards {
		id := uuid.NewString()
		ioType := ledgertypes.IOType_Incoming
		ioSubType := ledgertypes.IOSubType_MiningBenefit
		reqs = append(reqs, &statementmwpb.StatementReq{
			ID:         &id,
			AppID:      &reward.AppID,
			UserID:     &reward.UserID,
			CoinTypeID: &good.CoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &reward.Amount,
			IOExtra:    &reward.Extra,
			CreatedAt:  &good.LastRewardAt,
		})
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
	state := goodtypes.BenefitState_BenefitDone
	if _, err := goodmwcli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:          &good.ID,
		RewardState: &state,
	}); err != nil {
		return err
	}
	return nil
}

func (p *handler) Update(ctx context.Context, good interface{}, retry, notif chan interface{}) error {
	_good, ok := good.(*types.PersistentGood)
	if !ok {
		return fmt.Errorf("invalid good")
	}

	if _good.StatementExist {
		if err := p.updateGood(ctx, _good); err != nil {
			retry1.Retry(ctx, _good, retry)
			return err
		}
		return nil
	}

	if len(_good.OrderRewards) == 0 {
		if _, err := goodstmwcli.CreateGoodStatement(ctx, &goodstmwpb.GoodStatementReq{
			GoodID:                    &_good.ID,
			CoinTypeID:                &_good.CoinTypeID,
			TotalAmount:               &_good.TotalRewardAmount,
			UnsoldAmount:              &_good.UnsoldRewardAmount,
			TechniqueServiceFeeAmount: &_good.TechniqueFeeAmount,
			BenefitDate:               &_good.LastRewardAt,
		}); err != nil {
			retry1.Retry(ctx, _good, retry)
			return err
		}
		_good.StatementExist = true
		if err := p.updateGood(ctx, _good); err != nil {
			retry1.Retry(ctx, _good, retry)
			return err
		}
		return nil
	}

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})
	p.withCreateGoodLedgerStatement(sagaDispose, _good)
	p.withCreateLedgerStatements(sagaDispose, _good)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		retry1.Retry(ctx, _good, retry)
		return err
	}
	_good.StatementExist = true
	if err := p.updateGood(ctx, _good); err != nil {
		retry1.Retry(ctx, _good, retry)
		return err
	}

	return nil
}
