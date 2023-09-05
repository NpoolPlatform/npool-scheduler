package persistent

import (
	"context"
	"fmt"

	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/fail/returnbalance/types"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/shopspring/decimal"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateWithdrawState(dispose *dtmcli.SagaDispose, withdraw *types.PersistentWithdraw) {
	state := ledgertypes.WithdrawState_TransactionFail
	rollback := true
	req := &withdrawmwpb.WithdrawReq{
		ID:       &withdraw.ID,
		State:    &state,
		Rollback: &rollback,
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.withdraw.v2.Middleware/UpdateWithdraw",
		"ledger.middleware.withdraw.v2.Middleware/UpdateWithdraw",
		&withdrawmwpb.UpdateWithdrawRequest{
			Info: req,
		},
	)
}

func (p *handler) withReturnLockedBalance(dispose *dtmcli.SagaDispose, withdraw *types.PersistentWithdraw) {
	balance := decimal.RequireFromString(withdraw.LockedBalanceAmount)
	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return
	}

	req := &ledgermwpb.LedgerReq{
		AppID:      &withdraw.AppID,
		UserID:     &withdraw.UserID,
		CoinTypeID: &withdraw.CoinTypeID,
		Spendable:  &withdraw.LockedBalanceAmount,
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.ledger.v2.Middleware/AddBalance",
		"",
		&ledgermwpb.SubBalanceRequest{
			Info: req,
		},
	)
}

func (p *handler) Update(ctx context.Context, withdraw interface{}, retry, notif, done chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})
	p.withUpdateWithdrawState(sagaDispose, _withdraw)
	p.withReturnLockedBalance(sagaDispose, _withdraw)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		retry1.Retry(ctx, _withdraw, retry)
		return err
	}

	asyncfeed.AsyncFeed(ctx, _withdraw, done)

	return nil
}
