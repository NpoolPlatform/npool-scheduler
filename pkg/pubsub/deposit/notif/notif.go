package notif

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	statementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	"github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	"github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
)

func Prepare(body string) (interface{}, error) {
	req := statementmwpb.StatementReq{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*statementmwpb.StatementReq)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	user, err := usermwcli.GetUser(ctx, *in.AppID, *in.UserID)
	if err != nil {
		return err
	}
	if user == nil {
		return fmt.Errorf("invalid user")
	}
	now := uint32(time.Now().Unix())

	type b struct {
		CoinName string
		Address  string
	}
	var _b b
	if err := json.Unmarshal([]byte(*in.IOExtra), &_b); err != nil {
		return err
	}

	coin, err := coinmwcli.GetCoin(ctx, *in.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	if _, err := notifmwcli.GenerateNotifs(ctx, &notif.GenerateNotifsRequest{
		AppID:     *in.AppID,
		UserID:    *in.UserID,
		EventType: basetypes.UsedFor_DepositReceived,
		NotifType: basetypes.NotifType_NotifUnicast,
		Vars: &template.TemplateVars{
			Username:  &user.Username,
			Amount:    in.Amount,
			CoinUnit:  &coin.Unit,
			Address:   &_b.Address,
			Timestamp: &now,
		},
	}); err != nil {
		return err
	}

	return nil
}
