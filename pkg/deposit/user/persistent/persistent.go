package persistent

import (
	"context"
	"fmt"

	accountsvcname "github.com/NpoolPlatform/account-middleware/pkg/servicename"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user/types"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateAccount(dispose *dtmcli.SagaDispose, account *types.PersistentAccount) {
	req := &depositaccmwpb.AccountReq{
		ID:       &account.ID,
		Incoming: &account.DepositAmount,
	}
	dispose.Add(
		accountsvcname.ServiceDomain,
		"account.middleware.deposit.v1.Middleware/AddBalance",
		"account.middleware.deposit.v1.Middleware/SubBalance",
		&depositaccmwpb.AddBalanceRequest{
			Info: req,
		},
	)
}

func (p *handler) statement(account *types.PersistentAccount) *statementmwpb.StatementReq {
	id := uuid.NewString()
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_Deposit

	return &statementmwpb.StatementReq{
		ID:         &id,
		AppID:      &account.AppID,
		UserID:     &account.UserID,
		CoinTypeID: &account.CoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &account.DepositAmount,
		IOExtra:    &account.Extra,
	}
}

func (p *handler) withCreateStatement(dispose *dtmcli.SagaDispose, account *types.PersistentAccount) {
	req := p.statement(account)
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger1.statement.v2.Middleware/CreateStatement",
		"ledger.middleware.ledger1.statement.v2.Middleware/DeleteStatement",
		&statementmwpb.CreateStatementRequest{
			Info: req,
		},
	)
}

func (p *handler) notifyDeposit(account *types.PersistentAccount) {
	if err := pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		req := p.statement(account)
		return publisher.Update(
			basetypes.MsgID_DepositReceivedReq.String(),
			nil,
			nil,
			nil,
			req,
		)
	}); err != nil {
		logger.Sugar().Errorw(
			"notifDeposit",
			"AppID", account.AppID,
			"UserID", account.UserID,
			"Account", account.CoinTypeID,
			"AccountType", account.DepositAmount,
			"Error", err,
		)
	}
}

func (p *handler) Update(ctx context.Context, account interface{}, retry chan interface{}) error {
	_account, ok := account.(*types.PersistentAccount)
	if !ok {
		return fmt.Errorf("invalid account")
	}

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})
	p.withUpdateAccount(sagaDispose, _account)
	p.withCreateStatement(sagaDispose, _account)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	p.notifyDeposit(_account)

	return nil
}
