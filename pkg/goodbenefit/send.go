package goodbenefit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodcli "github.com/NpoolPlatform/good-middleware/pkg/client/appgood"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	npool "github.com/NpoolPlatform/message/npool"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	"github.com/NpoolPlatform/message/npool/good/mgr/v1/appgood"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	notifbenefitpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	tmplmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/template"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	notifbenefitcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/goodbenefit"
)

func send(ctx context.Context, channel basetypes.NotifChannel) {
	offset := int32(0)
	limit := int32(1000)

	t := time.Now()
	currentDayStart := uint32(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix())

	for {
		goodBenefits, _, err := notifbenefitcli.GetGoodBenefits(ctx, &notifbenefitpb.Conds{
			Notified:         &basetypes.BoolVal{Op: cruder.EQ, Value: false},
			BenefitDateStart: &basetypes.Uint32Val{Op: cruder.GTE, Value: currentDayStart},
		}, offset, limit)
		if err != nil {
			return
		}
		if len(goodBenefits) == 0 {
			break
		}

		content := "GoodID,GoodName,Amount,State,Message,BenefitDate,TxID,Notified\n"
		benefitIDs := []string{}
		goodIDs := []string{}
		for _, benefit := range goodBenefits {
			goodIDs = append(goodIDs, benefit.GoodID)
			content += fmt.Sprintf(`%v,%v,%v,%v,%v,%v,%v,%v\n`,
				benefit.GoodID, benefit.GoodName,
				benefit.Amount, benefit.State,
				benefit.Message, benefit.BenefitDate,
				benefit.TxID, benefit.Notified,
			)
			benefitIDs = append(benefitIDs, benefit.ID)
		}
		logger.Sugar().Infow("Content", content)

		goods, _, err := appgoodcli.GetGoods(ctx, &appgood.Conds{
			GoodIDs: &npool.StringSliceVal{
				Op:    cruder.IN,
				Value: goodIDs,
			},
		}, 0, 10000)
		if err != nil {
			logger.Sugar().Errorw("GetGoods", "error", err)
		}

		if len(goods) == 0 {
			break
		}

		// find AppID from Goods
		appIDs := []string{}
		for _, _good := range goods {
			appIDs = append(appIDs, _good.AppID)
		}

		_benefitIDs, err := json.Marshal(benefitIDs)
		if err != nil {
			logger.Sugar().Errorf("Marshal", "Error", err)
		}
		extra := fmt.Sprintf(`{"GoodBenefitIDs":"%v"}`, string(_benefitIDs))

		for _, appID := range appIDs {
			if _, err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
				AppID:     appID,
				EventType: basetypes.UsedFor_GoodBenefit,
				Extra:     &extra,
				NotifType: basetypes.NotifType_NotifMulticast,
				Vars: &tmplmwpb.TemplateVars{
					Message: &content,
				},
			}); err != nil {
				logger.Sugar().Errorw("GenerateNotifs", "Error", err)
			}
		}

		offset += limit
	}
}
