package notif

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	accountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/account"
	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	withdrawmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/withdraw"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	templatemwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
)

func Prepare(body string) (interface{}, error) {
	req := withdrawmwpb.Withdraw{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, mid string, req interface{}) error {
	in, ok := req.(*withdrawmwpb.Withdraw)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	user, err := usermwcli.GetUser(ctx, in.AppID, in.UserID)
	if err != nil {
		return err
	}
	if user == nil {
		return fmt.Errorf("invalid user")
	}
	now := uint32(time.Now().Unix())
	coin, err := coinmwcli.GetCoin(ctx, in.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	account, err := accountmwcli.GetAccount(ctx, in.AccountID)
	if err != nil {
		return err
	}
	if account == nil {
		return fmt.Errorf("invalid account")
	}

	eventType := basetypes.UsedFor_WithdrawalRequest
	if mid == basetypes.MsgID_WithdrawSuccessReq.String() {
		eventType = basetypes.UsedFor_WithdrawalCompleted
	}

	if _, err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
		AppID:     in.AppID,
		UserID:    in.UserID,
		EventType: eventType,
		NotifType: basetypes.NotifType_NotifUnicast,
		Vars: &templatemwpb.TemplateVars{
			Username:  &user.Username,
			Amount:    &in.Amount,
			CoinUnit:  &coin.Unit,
			Address:   &account.Address,
			Timestamp: &now,
		},
	}); err != nil {
		return err
	}

	return nil
}
