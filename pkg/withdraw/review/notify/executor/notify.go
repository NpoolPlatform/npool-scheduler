package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	// cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	accountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/account"
	usermwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	// basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	ledgerwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	// constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/types"
)

type withdrawReviewNotifyHandler struct {
	withdraws  []*ledgerwithdrawmwpb.Withdraw
	accounts   map[string]*accountmwpb.Account
	users      map[string]*usermwpb.User
	coins      map[string]*coinmwpb.Coin
	persistent chan interface{}
	notif      chan interface{}
	done       chan interface{}
}

func (h *withdrawReviewNotifyHandler) getAccounts(ctx context.Context) error {
	return nil
}

func (h *withdrawReviewNotifyHandler) getAppUsers(ctx context.Context) error {
	return nil
}

func (h *withdrawReviewNotifyHandler) getCoins(ctx context.Context) error {
	return nil
}

//nolint:gocritic
func (h *withdrawReviewNotifyHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Withdraws", h.withdraws,
			"Error", *err,
		)
	}
	persistentWithdrawReviewNotify := &types.PersistentWithdrawReviewNotify{
		Withdraws: h.withdraws,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentWithdrawReviewNotify, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentWithdrawReviewNotify, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentWithdrawReviewNotify, h.done)
}

//nolint:gocritic
func (h *withdrawReviewNotifyHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getAccounts(ctx); err != nil {
		return err
	}
	if err = h.getAppUsers(ctx); err != nil {
		return err
	}
	if err = h.getCoins(ctx); err != nil {
		return err
	}

	return nil
}
