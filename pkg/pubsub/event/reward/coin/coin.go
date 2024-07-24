package coin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	inspiremwsvcname "github.com/NpoolPlatform/inspire-middleware/pkg/servicename"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	coinallocatedmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/coin/allocated"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	dtm1 "github.com/NpoolPlatform/npool-scheduler/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
)

type calculateHandler struct {
	req *eventmwpb.CoinRewardRequest
}

func (h *calculateHandler) withCreateCoin(dispose *dtmcli.SagaDispose) {
	if h.req.CoinRewards == nil {
		return
	}
	extra := fmt.Sprintf(
		`{"TaskUserID":"%v"}`,
		h.req.TaskUserID,
	)
	for _, coin := range h.req.CoinRewards {
		id := uuid.NewString()
		req := &coinallocatedmwpb.CoinAllocatedReq{
			EntID:        &id,
			AppID:        &coin.AppID,
			UserID:       &coin.UserID,
			CoinConfigID: &coin.CoinConfigID,
			CoinTypeID:   &coin.CoinTypeID,
			Value:        &coin.CoinRewards,
			Extra:        &extra,
		}
		dispose.Add(
			inspiremwsvcname.ServiceDomain,
			"inspire.middleware.coin.allocated.v1.Middleware/CreateCoinAllocated",
			"inspire.middleware.coin.allocated.v1.Middleware/DeleteCoinAllocated",
			&coinallocatedmwpb.CreateCoinAllocatedRequest{
				Info: req,
			},
		)
	}
}

func (h *calculateHandler) withCreateLedgerStatements(dispose *dtmcli.SagaDispose, ev *eventmwpb.Event) {
	if h.req.CoinRewards == nil {
		return
	}
	rollback := true
	ioType := ledgertypes.IOType_Incoming
	ioSubType := ledgertypes.IOSubType_SimulateMiningBenefit
	reqs := []*statementmwpb.StatementReq{}
	for _, coin := range h.req.CoinRewards {
		id := uuid.NewString()
		now := uint32(time.Now().Unix())
		ioExtra := fmt.Sprintf(
			`{"EventID":"%v","EventType":"%v","TaskID":"%v","TaskUserID":"%v"}`,
			ev.EntID,
			ev.EventType,
			h.req.TaskID,
			h.req.TaskUserID,
		)
		reqs = append(reqs, &statementmwpb.StatementReq{
			EntID:      &id,
			AppID:      &coin.AppID,
			UserID:     &coin.UserID,
			CoinTypeID: &coin.CoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &coin.CoinRewards,
			IOExtra:    &ioExtra,
			CreatedAt:  &now,
			Rollback:   &rollback,
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

func Prepare(body string) (interface{}, error) {
	req := eventmwpb.CoinRewardRequest{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*eventmwpb.CoinRewardRequest)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	handler := &calculateHandler{
		req: in,
	}

	ev, err := eventmwcli.GetEvent(ctx, in.EventID)
	if err != nil {
		return err
	}

	const timeoutSeconds = 30
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
		TimeoutToFail:  timeoutSeconds,
	})
	// create coin
	handler.withCreateCoin(sagaDispose)
	handler.withCreateLedgerStatements(sagaDispose, ev)

	if err := dtm1.Do(ctx, sagaDispose); err != nil {
		return err
	}

	return nil
}
