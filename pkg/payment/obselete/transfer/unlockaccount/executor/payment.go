package executor

import (
	"context"

	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/transfer/unlockaccount/types"
)

type paymentHandler struct {
	*paymentmwpb.Payment
	persistent      chan interface{}
	done            chan interface{}
	paymentAccounts map[string]*paymentaccountmwpb.Account
	unlockAccountID *uint32
}

func (h *paymentHandler) getPaymentAccounts(ctx context.Context) (err error) {
	h.paymentAccounts, err = schedcommon.GetPaymentAccounts(ctx, func() (accountIDs []string) {
		for _, paymentTransfer := range h.PaymentTransfers {
			accountIDs = append(accountIDs, paymentTransfer.AccountID)
		}
		return
	}())
	return wlog.WrapError(err)
}

func (h *paymentHandler) resolveUnlockAccountID() {
	for _, paymentAccount := range h.paymentAccounts {
		h.unlockAccountID = &paymentAccount.ID
		return
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
		Payment:         h.Payment,
		UnlockAccountID: h.unlockAccountID,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentPayment, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentPayment, h.done)
}

//nolint:gocritic
func (h *paymentHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getPaymentAccounts(ctx); err != nil {
		return err
	}
	h.resolveUnlockAccountID()

	return nil
}
