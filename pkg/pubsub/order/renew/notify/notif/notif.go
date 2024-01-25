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
	content := `<table style="border-collapse: collapse;">`
	content += "<tr>"
	content += `  <td colspan="4">Estimated Deductions</td>`
	content += "</tr>"
	content += "<tr>"
	content += "  <td>Order ID</td>"
	// TODO: add order link and some other order info
	content += `  <td colspan="3"><strong>` + req.ParentOrder.EntID + `</strong></td>`
	content += "</tr>"
	content += "<tr>"
	content += "  <th>CoinName</th>"
	content += "  <th>Blockchain</th>"
	content += "  <th>USDT Currency</th>"
	content += "  <th>Deduction Amount</th>"
	content += "</tr>"
	for _, deduction := range req.Deductions {
		content += "<tr>"
		content += `  <td>` + deduction.AppCoin.Unit + `</td>`
		content += `  <td>` + deduction.AppCoin.Name + `</td>`
		content += `  <td>` + deduction.USDCurrency + `</td>`
		content += `  <td>` + deduction.Amount + `</td>`
		content += "</tr>"
	}
	content += "<tr>"
	content += `  <td colspan="5">Renew Candidates</td>`
	content += "</tr>"
	content += "<tr>"
	content += "  <th>Product Name</th>"
	content += "  <th>Price</th>"
	content += "  <th>Least Duration</th>"
	content += "  <th>Units</th>"
	content += "  <th>EndAt</th>"
	content += "</tr>"
	for _, renewInfo := range req.RenewInfos {
		content += "<tr>"
		content += `  <td>` + renewInfo.AppGood.GoodName + `</td>`
		content += `  <td>` + renewInfo.AppGood.UnitPrice + `</td>`
		content += `  <td>` + fmt.Sprintf("%v", renewInfo.AppGood.MinOrderDuration) + `</td>`
		content += `  <td>` + req.ParentOrder.Units + `</td>`
		content += `  <td>` + fmt.Sprintf("%v", renewInfo.EndAt) + `</td>`
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
