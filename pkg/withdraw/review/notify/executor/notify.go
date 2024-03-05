package executor

import (
	"context"

	accountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/account"
	appmwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/app"
	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	accountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/account"
	appmwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/app"
	usermwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	ledgerwithdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	withdrawreviewnotifypb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/withdraw/review/notify"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/types"
)

type withdrawReviewNotifyHandler struct {
	withdraws        []*ledgerwithdrawmwpb.Withdraw
	accounts         map[string]*accountmwpb.Account
	apps             map[string]*appmwpb.App
	users            map[string]*usermwpb.User
	coins            map[string]*coinmwpb.Coin
	appWithdrawInfos []*withdrawreviewnotifypb.AppWithdrawInfos
	persistent       chan interface{}
	notif            chan interface{}
	done             chan interface{}
}

func (h *withdrawReviewNotifyHandler) getAccounts(ctx context.Context) error {
	accountIDs := []string{}
	for _, withdraw := range h.withdraws {
		accountIDs = append(accountIDs, withdraw.AccountID)
	}

	accounts, _, err := accountmwcli.GetAccounts(ctx, &accountmwpb.Conds{
		EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: accountIDs},
	}, 0, int32(len(accountIDs)))
	if err != nil {
		return err
	}

	h.accounts = map[string]*accountmwpb.Account{}
	for _, account := range accounts {
		h.accounts[account.EntID] = account
	}

	return nil
}

func (h *withdrawReviewNotifyHandler) getApps(ctx context.Context) error {
	appIDs := []string{}
	for _, withdraw := range h.withdraws {
		appIDs = append(appIDs, withdraw.AppID)
	}

	apps, _, err := appmwcli.GetApps(ctx, &appmwpb.Conds{
		EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: appIDs},
	}, 0, int32(len(appIDs)))
	if err != nil {
		return err
	}

	h.apps = map[string]*appmwpb.App{}
	for _, app := range apps {
		h.apps[app.EntID] = app
	}

	return nil
}

func (h *withdrawReviewNotifyHandler) getAppUsers(ctx context.Context) error {
	userIDs := []string{}
	for _, withdraw := range h.withdraws {
		userIDs = append(userIDs, withdraw.UserID)
	}

	users, _, err := usermwcli.GetUsers(ctx, &usermwpb.Conds{
		EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: userIDs},
	}, 0, int32(len(userIDs)))
	if err != nil {
		return err
	}

	h.users = map[string]*usermwpb.User{}
	for _, user := range users {
		h.users[user.EntID] = user
	}

	return nil
}

func (h *withdrawReviewNotifyHandler) getCoins(ctx context.Context) error {
	coinTypeIDs := []string{}
	for _, withdraw := range h.withdraws {
		coinTypeIDs = append(coinTypeIDs, withdraw.CoinTypeID)
	}

	coins, _, err := coinmwcli.GetCoins(ctx, &coinmwpb.Conds{
		EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
	}, 0, int32(len(coinTypeIDs)))
	if err != nil {
		return err
	}

	h.coins = map[string]*coinmwpb.Coin{}
	for _, coin := range coins {
		h.coins[coin.EntID] = coin
	}

	return nil
}

func (h *withdrawReviewNotifyHandler) resolveWithdrawInfos() {
	appWithdrawInfos := map[string]*withdrawreviewnotifypb.AppWithdrawInfos{}

	for _, withdraw := range h.withdraws {
		account, ok := h.accounts[withdraw.AccountID]
		if !ok {
			continue
		}
		user, ok := h.users[withdraw.UserID]
		if !ok {
			continue
		}
		coin, ok := h.coins[withdraw.CoinTypeID]
		if !ok {
			continue
		}
		app, ok := h.apps[withdraw.AppID]
		if !ok {
			continue
		}

		withdrawInfos, ok := appWithdrawInfos[withdraw.AppID]
		if !ok {
			withdrawInfos = &withdrawreviewnotifypb.AppWithdrawInfos{
				AppID:   withdraw.AppID,
				AppName: app.Name,
			}
		}

		withdrawInfos.Withdraws = append(withdrawInfos.Withdraws, &withdrawreviewnotifypb.WithdrawInfo{
			Withdraw: withdraw,
			Account:  account,
			User:     user,
			Coin:     coin,
		})
	}

	for _, _appWithdrawInfos := range appWithdrawInfos {
		h.appWithdrawInfos = append(h.appWithdrawInfos, _appWithdrawInfos)
	}
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
		AppWithdraws: h.appWithdrawInfos,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentWithdrawReviewNotify, h.notif)
		asyncfeed.AsyncFeed(ctx, persistentWithdrawReviewNotify, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentWithdrawReviewNotify, h.done)
}

//nolint:gocritic
func (h *withdrawReviewNotifyHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getAccounts(ctx); err != nil {
		return err
	}
	if err = h.getApps(ctx); err != nil {
		return err
	}
	if err = h.getAppUsers(ctx); err != nil {
		return err
	}
	if err = h.getCoins(ctx); err != nil {
		return err
	}
	h.resolveWithdrawInfos()

	return nil
}
