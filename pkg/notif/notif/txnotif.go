package notif

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/message/npool/third/mgr/v1/usedfor"

	txnotifcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/txnotifstate"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
	txnotifmgrpb "github.com/NpoolPlatform/message/npool/notif/mgr/v1/notif/txnotifstate"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"

	useraccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/user"
	useraccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/user"
)

//nolint:gocognit
func sendTxNotif(ctx context.Context) {
	offset := int32(0)
	limit := int32(50)
	for {
		txNotifs, _, err := txnotifcli.GetTxNotifStates(ctx, &txnotifmgrpb.Conds{
			NotifState: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(txnotifmgrpb.TxState_WaitTxSuccess.Number()),
			},
			NotifType: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(txnotifmgrpb.TxType_Withdraw.Number()),
			},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "offset", offset, "limit", limit, "error", err)
			return
		}
		offset += limit
		if len(txNotifs) == 0 {
			return
		}
		txNotifMap := map[string]*txnotifmgrpb.TxNotifState{}
		txIDs := []string{}
		for _, val := range txNotifs {
			txIDs = append(txIDs, val.TxID)
			txNotifMap[val.TxID] = val
		}

		txInfos, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
			IDs: &commonpb.StringSliceVal{
				Op:    cruder.IN,
				Value: txIDs,
			},
		}, 0, int32(len(txNotifs)))
		if err != nil {
			logger.Sugar().Errorw("sendNotif", "offset", offset, "limit", limit, "error", err)
			return
		}
		for _, val := range txInfos {
			if val.State != txmgrpb.TxState_StateSuccessful {
				continue
			}

			accountInfo, err := useraccmwcli.GetAccountOnly(ctx, &useraccmwpb.Conds{
				AccountID: &commonpb.StringVal{
					Op:    cruder.EQ,
					Value: val.ToAccountID,
				},
			})
			if err != nil {
				continue
			}
			if accountInfo == nil {
				continue
			}
			extra := fmt.Sprintf(`{"TxID":"%v","AccountId":"%v"}`, val.ID, accountInfo.ID)

			CreateNotif(
				ctx,
				accountInfo.AppID,
				accountInfo.UserID,
				extra,
				&val.Amount,
				&val.CoinUnit,
				&accountInfo.Address,
				usedfor.UsedFor_WithdrawalCompleted,
			)
			txNotif, ok := txNotifMap[val.ID]
			if !ok {
				logger.Sugar().Errorw("sendNotif", "error", "txNotifMap not exist", "TxID", val.ID)
				continue
			}
			state := txnotifmgrpb.TxState_AlreadySend
			_, err = txnotifcli.UpdateTxNotifState(ctx, &txnotifmgrpb.TxNotifStateReq{
				ID:         &txNotif.ID,
				NotifState: &state,
			})
			if err != nil {
				logger.Sugar().Errorw("sendNotif", "offset", offset, "limit", limit, "error", err)
				return
			}
		}
	}
}
