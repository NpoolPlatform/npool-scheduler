package goodbenefit

import (
	"context"
	"fmt"

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

//nolint
func send(ctx context.Context, channel basetypes.NotifChannel) {
	offset := int32(0)
	limit := int32(1000)

	for {
		goodBenefits, _, err := notifbenefitcli.GetGoodBenefits(ctx, &notifbenefitpb.Conds{
			Generated: &basetypes.BoolVal{Op: cruder.EQ, Value: false},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("GetGoodBenefits:", "Error", err)
			return
		}
		if len(goodBenefits) == 0 {
			logger.Sugar().Info("goodbenefits:", "length:", 0)
			break
		}

		content := "GoodID,GoodName,Amount,State,Message,TxID,BenefitDate\n"
		benefitIDs := []string{}
		goodIDs := []string{}
		for _, benefit := range goodBenefits {
			goodIDs = append(goodIDs, benefit.GoodID)
			content += fmt.Sprintf(`%v,%v,%v,%v,%v,%v,%v\n`,
				benefit.GoodID, benefit.GoodName,
				benefit.Amount, benefit.State,
				benefit.Message, benefit.TxID,
				benefit.BenefitDate,
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
			logger.Sugar().Errorw("GetGoods", "length", len(goods))
			break
		}

		// find AppID from Goods
		appIDs := []string{}
		for _, _good := range goods {
			appIDs = append(appIDs, _good.AppID)
		}

		if err != nil {
			logger.Sugar().Errorf("Marshal", "Error", err)
		}

		logger.Sugar().Info("----------------------content------------------", content)
		for _, appID := range appIDs {
			_, err := notifmwcli.GenerateNotifs(ctx, &notifmwpb.GenerateNotifsRequest{
				AppID:     appID,
				EventType: basetypes.UsedFor_GoodBenefit,
				NotifType: basetypes.NotifType_NotifMulticast,
				Vars: &tmplmwpb.TemplateVars{
					Message: &content,
				},
			})
			if err != nil {
				logger.Sugar().Errorw("GenerateNotifs", "Error", err)
			}

			generated := true
			for _, benefitID := range benefitIDs {
				_, err := notifbenefitcli.UpdateGoodBenefit(ctx, &notifbenefitpb.GoodBenefitReq{
					ID:        &benefitID,
					Generated: &generated,
				})
				if err != nil {
					logger.Sugar().Errorw("UpdateGoodBenefit", "Error", err)
				}
			}
		}

		offset += limit
	}
}
