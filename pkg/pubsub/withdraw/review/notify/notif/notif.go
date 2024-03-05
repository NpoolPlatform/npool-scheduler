package notif

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	usermwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	templatemwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	withdrawreviewnotifypb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/withdraw/review/notify"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
)

func Prepare(body string) (interface{}, error) {
	req := withdrawreviewnotifypb.MsgWithdrawReviewNotifyReq{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func userAccount(user *usermwpb.User) string {
	account := user.EmailAddress
	if user.PhoneNO != "" {
		account += " | " + user.PhoneNO
	}
	return account
}

//nolint:goconst
func appWithdraws2Content(appWithdraws *withdrawreviewnotifypb.AppWithdrawInfos) string {
	content := `<table style="border-collapse: collapse; text-align: left;">`
	content += "<tr>"
	content += `  <td style="border: 1px solid #dddddd;" colspan="4">Withdraw Requests</td>`
	content += "</tr>"
	content += "<tr>"
	content += `  <td style="border: 1px solid #dddddd;">Application</td>`
	content += `  <td style="border: 1px solid #dddddd;" colspan="3"><strong>` + appWithdraws.AppName + `</strong></td>`
	content += "</tr>"
	content += "<tr>"
	content += `  <th style="border: 1px solid #dddddd;">From UserID</th>`
	content += `  <th style="border: 1px solid #dddddd;">From User Account</th>`
	content += `  <th style="border: 1px solid #dddddd;">To Address</th>`
	content += `  <th style="border: 1px solid #dddddd;">Currency</th>`
	content += `  <th style="border: 1px solid #dddddd;">Amount</th>`
	content += `  <th style="border: 1px solid #dddddd;">Created At</th>`
	content += "</tr>"
	for _, withdraw := range appWithdraws.Withdraws {
		content += "<tr>"
		content += `  <td style="border: 1px solid #dddddd;">` + withdraw.Withdraw.UserID + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + userAccount(withdraw.User) + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + withdraw.Account.Address + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + withdraw.Coin.Unit + ` | ` + withdraw.Coin.Name + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + withdraw.Withdraw.Amount + `</td>`
		content += `  <td style="border: 1px solid #dddddd;">` + fmt.Sprintf("%v", time.Unix(int64(withdraw.Withdraw.CreatedAt), 0)) + `</td>`
		content += "</tr>"
	}
	content += "</table>"
	return content
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*withdrawreviewnotifypb.MsgWithdrawReviewNotifyReq)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	for _, appWithdraws := range in.AppWithdraws {
		content := appWithdraws2Content(appWithdraws)
		if _, err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
			AppID:     appWithdraws.AppID,
			EventType: basetypes.UsedFor_WithdrawReviewNotify,
			NotifType: basetypes.NotifType_NotifMulticast,
			Vars: &templatemwpb.TemplateVars{
				Message: &content,
			},
		}); err != nil {
			return err
		}
	}

	return nil
}
