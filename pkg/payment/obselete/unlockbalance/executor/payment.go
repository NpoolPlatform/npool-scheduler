package executor

import (
	"context"

	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/unlockbalance/types"

	"github.com/google/uuid"
)

type paymentHandler struct {
	*paymentmwpb.Payment
	persistent   chan interface{}
	notif        chan interface{}
	done         chan interface{}
	ledgerLockID *uuid.UUID
}

func (h *paymentHandler) resolveLedgerLockID() {
	if uid, err := uuid.Parse(h.LedgerLockID); err == nil && uid != uuid.Nil {
		h.ledgerLockID = &uid
	}
}

//nolint:gocritic
func (h *paymentHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Payment", h,
		)
	}
	persistentPayment := &types.PersistentPayment{
		Payment: h.Payment,
	}
	if h.ledgerLockID != nil {
		persistentPayment.XLedgerLockID = func() *string { s := h.ledgerLockID.String(); return &s }()
	}
	asyncfeed.AsyncFeed(ctx, persistentPayment, h.persistent)
}

//nolint:gocritic
func (h *paymentHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)
	h.resolveLedgerLockID()
	return nil
}
