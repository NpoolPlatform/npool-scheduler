package persistent

import (
	"context"
	"fmt"

	ledgergwname "github.com/NpoolPlatform/ledger-gateway/pkg/servicename"
	ledgersvcname "github.com/NpoolPlatform/ledger-middleware/pkg/servicename"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	reviewtypes "github.com/NpoolPlatform/message/npool/basetypes/review/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	reviewmwpb "github.com/NpoolPlatform/message/npool/review/mw/v2/review"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/created/types"
	reviewsvcname "github.com/NpoolPlatform/review-middleware/pkg/servicename"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"

	"github.com/google/uuid"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) withUpdateWithdrawState(dispose *dtmcli.SagaDispose, withdraw *types.PersistentWithdraw, reviewID string) {
	state := ledgertypes.WithdrawState_Reviewing
	req := &withdrawmwpb.WithdrawReq{
		ID:       &withdraw.ID,
		State:    &state,
		ReviewID: &reviewID,
	}
	dispose.Add(
		ledgersvcname.ServiceDomain,
		"ledger.middleware.withdraw.v2.Middleware/UpdateWithdraw",
		"",
		&withdrawmwpb.UpdateWithdrawRequest{
			Info: req,
		},
	)
}

func (p *handler) withCreateReview(dispose *dtmcli.SagaDispose, withdraw *types.PersistentWithdraw, reviewID string) {
	serviceName := ledgergwname.ServiceDomain
	objType := reviewtypes.ReviewObjectType_ObjectWithdrawal
	req := &reviewmwpb.ReviewReq{
		EntID:      &reviewID,
		AppID:      &withdraw.AppID,
		Domain:     &serviceName,
		ObjectType: &objType,
		ObjectID:   &withdraw.ID,
		Trigger:    &withdraw.ReviewTrigger,
	}
	dispose.Add(
		reviewsvcname.ServiceDomain,
		"review.middleware.review.v2.Middleware/CreateReview",
		"review.middleware.review.v2.Middleware/DeleteReview",
		&reviewmwpb.CreateReviewRequest{
			Info: req,
		},
	)
}

func (p *handler) Update(ctx context.Context, withdraw interface{}, notif, done chan interface{}) error {
	_withdraw, ok := withdraw.(*types.PersistentWithdraw)
	if !ok {
		return fmt.Errorf("invalid withdraw")
	}

	defer asyncfeed.AsyncFeed(ctx, _withdraw, done)

	const timeoutSeconds = 10
	sagaDispose := dtmcli.NewSagaDispose(dtmimp.TransOptions{
		WaitResult:     true,
		RequestTimeout: timeoutSeconds,
	})
	reviewID := uuid.NewString()
	p.withCreateReview(sagaDispose, _withdraw, reviewID)
	p.withUpdateWithdrawState(sagaDispose, _withdraw, reviewID)
	if err := dtmcli.WithSaga(ctx, sagaDispose); err != nil {
		return err
	}

	asyncfeed.AsyncFeed(ctx, _withdraw, notif)

	return nil
}
