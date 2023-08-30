package notif

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	basenotif "github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/deposit/user/types"

	"github.com/google/uuid"
)

type handler struct{}

func NewNotif() basenotif.Notify {
	return &handler{}
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

func (p *handler) notifyDeposit(account *types.PersistentAccount) error {
	return pubsub.WithPublisher(func(publisher *pubsub.Publisher) error {
		msgID := basetypes.MsgID_DepositReceivedReq.String()
		if account.Error != nil {
			msgID = basetypes.MsgID_DepositCheckFailReq.String()
		}
		if account.Error == nil {
			return publisher.Update(msgID, nil, nil, nil, p.statement(account))
		} else {
			req := &basetypes.MsgError{
				Error: account.Error.Error(),
			}
			value, _ := json.Marshal(p.statement(account))
			req.Value = string(value)
			return publisher.Update(msgID, nil, nil, nil, req)
		}
	})
}

func (p *handler) Notify(ctx context.Context, account interface{}, retry chan interface{}) error {
	_account, ok := account.(*types.PersistentAccount)
	if !ok {
		return fmt.Errorf("invalid account")
	}
	if err := p.notifyDeposit(_account); err != nil {
		logger.Sugar().Errorw(
			"notifDeposit",
			"AppID", _account.AppID,
			"UserID", _account.UserID,
			"Account", _account.CoinTypeID,
			"AccountType", _account.DepositAmount,
			"Error", err,
		)
		retry1.Retry(ctx, _account, retry)
		return err
	}
	return nil
}
