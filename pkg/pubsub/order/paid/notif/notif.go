package notif

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	templatemwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
)

func Prepare(body string) (interface{}, error) {
	req := ordermwpb.Order{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*ordermwpb.Order)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	coin, err := coinmwcli.GetCoin(ctx, in.PaymentCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid payment coin")
	}

	now := uint32(time.Now().Unix())
	if _, err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
		AppID:     in.AppID,
		UserID:    &in.UserID,
		EventType: basetypes.UsedFor_OrderCompleted,
		NotifType: basetypes.NotifType_NotifUnicast,
		Vars: &templatemwpb.TemplateVars{
			Amount:    &in.PaymentAmount,
			CoinUnit:  &coin.Unit,
			Timestamp: &now,
		},
	}); err != nil {
		return err
	}

	return nil
}
