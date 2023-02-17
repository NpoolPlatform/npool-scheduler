package notification

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	useraccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/user"
	usermgrpb "github.com/NpoolPlatform/message/npool/appuser/mgr/v2/appuser"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"
	notifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif"
	txnotifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif/tx"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	tmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"

	useraccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/user"
	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	txnotifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/tx"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
)

func waitSuccess(ctx context.Context) error {
	offset := int32(0)
	limit := int32(1000)

	for {
		notifs, _, err := txnotifmwcli.GetTxs(ctx, &txnotifmgrpb.Conds{
			NotifState: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(txnotifmgrpb.TxState_WaitSuccess.Number()),
			},
			TxType: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(basetypes.TxType_TxWithdraw.Number()),
			},
		})
		if err != nil {
			return err
		}
		if len(notifs) == 0 {
			break
		}

		tids := []string{}
		notifMap := map[string]*txnotifmgrpb.Tx{}

		for _, notif := range notifs {
			tids = append(tids, notif.TxID)
			notifMap[notif.TxID] = notif
		}

		txs, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
			IDs: &commonpb.StringSliceVal{
				Op:    cruder.IN,
				Value: tids,
			},
		}, 0, int32(len(tids)))
		if err != nil {
			return err
		}

		for _, tx := range txs {
			if tx.State != txmgrpb.TxState_StateSuccessful {
				continue
			}

			acc, err := useraccmwcli.GetAccountOnly(ctx, &useraccmwpb.Conds{
				AccountID: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: val.ToAccountID,
				},
			})
			if err != nil {
				return err
			}
			if acc == nil {
				continue
			}

			user, err := usermwcli.GetUser(ctx, acc.AppID, acc.UserID)
			if err != nil {
				return err
			}
			if user == nil {
				continue
			}

			extra := fmt.Sprintf(`{"TxID":"%v"}`, tx.ID)

			if err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
				AppID:     acc.AppID,
				UserID:    acc.UserID,
				EventType: basetypes.UsedFor_WithdrawalCompleted,
				Extra:     &extra,
				Vars: &tmplmwpb.TemplateVars{
					Username: &user.Username,
					Amount:   &tx.Amount,
					CoinUnit: &tx.CoinUnit,
					Address:  &acc.Address,
				},
			}); err != nil {
				return err
			}

			notif, ok := notifMap[tx.ID]
			if !ok {
				continue
			}

			state := txnotifmgrpb.TxState_WaitNotified
			_, err = txnotifmwcli.UpdateTx(ctx, &txnotifmgrpb.TxReq{
				ID:         &notif.ID,
				NotifState: &state,
			})
			if err != nil {
				return err
			}
		}

		offset += limit
	}

	return nil
}

func waitNotified(ctx context.Context) error {
	offset := int32(0)
	limit := int32(1000)

	for {
		notifs, _, err := txnotifmwcli.GetTxs(ctx, &txnotifmgrpb.Conds{
			NotifState: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(txnotifmgrpb.TxState_WaitNotified.Number()),
			},
			TxType: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(basetypes.TxType_TxWithdraw.Number()),
			},
		})
		if err != nil {
			return err
		}
		if len(notifs) == 0 {
			break
		}

		for _, notif := range notifs {
			_notif, err := notifmwcli.GetNotifOnly(ctx, &notifmgrpb.Conds{
				Extra: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: notif.TxID,
				},
			})
			if err != nil {
				return err
			}
			if _notif == nil {
				continue
			}

			if !_notif.Notified {
				continue
			}

			state := txnotifmgrpb.TxState_Notified
			_, err = txnotifmwcli.UpdateTx(ctx, &txnotifmgrpb.TxReq{
				ID:         &notif.ID,
				NotifState: &state,
			})
			if err != nil {
				return err
			}
		}

		offset += limit
	}

	return nil
}

func processTx(ctx context.Context) {
	if err := waitSuccess(ctx); err != nil {
		logger.Sugar().Errorw("processTx", "error", err)
		return
	}
	if err := waitNotified(ctx); err != nil {
		logger.Sugar().Errorw("processTx", "error", err)
		return
	}
}
