package notif

import (
	"context"
	"encoding/json"
	"fmt"

	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	templatemwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	orderrenewpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order/renew"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
)

func Prepare(body string) (interface{}, error) {
	req := orderrenewpb.MsgOrderChildsRenewReq{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func req2content(req *orderrenewpb.MsgOrderChildsRenewReq) string {
	return fmt.Sprintf("%v", req)
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*orderrenewpb.MsgOrderChildsRenewReq)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	content := req2content(in)

	eventType := basetypes.UsedFor_OrderChildsRenewNotify
	if in.WillCreateOrder {
		eventType = basetypes.UsedFor_OrderChildsRenew
	}

	reqs := &notifmwpb.GenerateMultiNotifsRequest{
		AppID: in.ParentOrder.AppID,
		Reqs: []*notifmwpb.GenerateMultiNotifsRequest_XNotifReq{
			{
				UserID:    &in.ParentOrder.UserID,
				EventType: eventType,
				NotifType: basetypes.NotifType_NotifUnicast,
				Vars: &templatemwpb.TemplateVars{
					Message: &content,
				},
			},
			{
				EventType: eventType,
				NotifType: basetypes.NotifType_NotifMulticast,
				Vars: &templatemwpb.TemplateVars{
					Message: &content,
				},
			},
		},
	}
	if _, err := notifmwcli.GenerateMultiNotifs(ctx, reqs); err != nil {
		return err
	}

	return nil
}
