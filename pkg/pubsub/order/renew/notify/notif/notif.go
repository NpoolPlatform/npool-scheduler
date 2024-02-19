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

//nolint:goconst
func req2content(req *orderrenewpb.MsgOrderChildsRenewReq) string {
	content := `<table style="border-collapse: collapse; text-align: left;">`
	if req.Error != nil {
		content += "<tr>"
		content += `  <td style="border: 1px solid #dddddd;" colspan="4">Error: ` + fmt.Sprintf("%v", *req.Error) + `</td>`
		content += "</tr>"
	}
	if req.InsufficientBalance {
		content += "<tr>"
		content += `  <td style="border: 1px solid #dddddd;" colspan="4">Insufficient Balance</td>`
		content += "</tr>"
	}
	content += "<tr>"
	content += `  <td style="border: 1px solid #dddddd;" colspan="4">Estimated Deductions</td>`
	content += "</tr>"
	content += "<tr>"
	content += `  <td style="border: 1px solid #dddddd;">Order ID</td>`
	// TODO: add order link and some other order info
	content += `  <td style="border: 1px solid #dddddd;" colspan="3"><strong>` + req.ParentOrder.EntID + `</strong></td>`
	content += "</tr>"
	content += "<tr>"
	content += `  <th style="border: 1px solid #dddddd;">CoinName</th>`
	content += `  <th style="border: 1px solid #dddddd;">Blockchain</th>`
	content += `  <th style="border: 1px solid #dddddd;">USDT Currency</th>`
	content += `  <th style="border: 1px solid #dddddd;">Deduction Amount</th>`
	content += "</tr>"
	for _, deduction := range req.Deductions {
		content += "<tr>"
		content += `  <td style="border: 1px solid #dddddd;">` + deduction.AppCoin.Unit + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + deduction.AppCoin.Name + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + deduction.USDCurrency + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + deduction.Amount + `</td>`
		content += "</tr>"
	}
	content += "<tr>"
	content += "</table>"

	content += `<table style="border-collapse: collapse; text-align: left;">`
	content += `  <td style="border: 1px solid #dddddd;" colspan="5">Renew Candidates</td>`
	content += "</tr>"
	content += "<tr>"
	content += `  <th style="border: 1px solid #dddddd;">Product Name</th>`
	content += `  <th style="border: 1px solid #dddddd;">Price</th>`
	content += `  <th style="border: 1px solid #dddddd;">Duration</th>`
	content += `  <th style="border: 1px solid #dddddd;">Units</th>`
	content += `  <th style="border: 1px solid #dddddd;">EndAt</th>`
	content += "</tr>"
	for _, renewInfo := range req.RenewInfos {
		content += "<tr>"
		content += `  <td style="border: 1px solid #dddddd;">` + renewInfo.AppGood.GoodName + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + renewInfo.AppGood.UnitPrice + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + fmt.Sprintf("%v", renewInfo.RenewDuration) + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + req.ParentOrder.Units + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + fmt.Sprintf("%v", renewInfo.EndAt) + `</td>`
		content += "</tr>"
	}
	content += "</table>"
	return content
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

	reqs := []*notifmwpb.GenerateMultiNotifsRequest_XNotifReq{
		{
			EventType: eventType,
			NotifType: basetypes.NotifType_NotifMulticast,
			Vars: &templatemwpb.TemplateVars{
				Message: &content,
			},
		},
	}
	if in.Error == nil {
		reqs = append(reqs, &notifmwpb.GenerateMultiNotifsRequest_XNotifReq{
			UserID:    &in.ParentOrder.UserID,
			EventType: eventType,
			NotifType: basetypes.NotifType_NotifUnicast,
			Vars: &templatemwpb.TemplateVars{
				Message: &content,
			},
		})
	}

	if _, err := notifmwcli.GenerateMultiNotifs(ctx, &notifmwpb.GenerateMultiNotifsRequest{
		AppID: in.ParentOrder.AppID,
		Reqs:  reqs,
	}); err != nil {
		return err
	}

	return nil
}
