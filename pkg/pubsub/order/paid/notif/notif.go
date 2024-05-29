package notif

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	templatemwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	schedorderpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
)

func Prepare(body string) (interface{}, error) {
	req := schedorderpb.OrderInfo{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*schedorderpb.OrderInfo)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	// TODO: generate payment table

	now := uint32(time.Now().Unix())
	if _, err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
		AppID:     in.AppID,
		UserID:    &in.UserID,
		EventType: basetypes.UsedFor_OrderCompleted,
		NotifType: basetypes.NotifType_NotifUnicast,
		Vars: &templatemwpb.TemplateVars{
			Amount:    &in.PaymentAmountUSD,
			Timestamp: &now,
		},
	}); err != nil {
		return err
	}

	return nil
}
