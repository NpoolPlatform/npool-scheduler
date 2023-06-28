package goodbenefit

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	notifbenefitpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	notifbenefitcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/goodbenefit"
)

func CreateGoodBenefit(ctx context.Context, in *notifbenefitpb.GoodBenefitReq) {
	info, err := notifbenefitcli.CreateGoodBenefit(ctx, &notifbenefitpb.GoodBenefitReq{
		GoodID:      in.GoodID,
		GoodName:    in.GoodName,
		Amount:      in.Amount,
		State:       in.State,
		Message:     in.Message,
		BenefitDate: in.BenefitDate,
		TxID:        in.TxID,
		Notified:    in.Notified,
	})
	if err != nil {
		logger.Sugar().Errorw("CreateGoodBenefit", "Error", err)
	}
	logger.Sugar().Info("Info", info)
}
