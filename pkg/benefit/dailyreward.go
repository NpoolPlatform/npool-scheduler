package benefit

import (
	"context"
	"fmt"

	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/appgood"
	appgoodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/appgood"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
)

func UpdateDailyReward(ctx context.Context, good *Good) error {
	total, err := decimal.NewFromString(good.GetGoodTotal())
	if err != nil {
		return err
	}
	if total.Cmp(decimal.NewFromInt(0)) == 0 {
		return fmt.Errorf("invalid stock")
	}

	dailyReward := good.TodayRewardAmount.Div(total).String()

	offset := int32(0)
	limit := int32(100)

	for {
		appGoods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmgrpb.Conds{
			GoodID: &commonpb.StringVal{Op: cruder.EQ, Value: good.ID},
		}, offset, limit)
		if err != nil {
			return err
		}
		offset += limit

		if len(appGoods) == 0 {
			break
		}

		for _, val := range appGoods {
			_, err = appgoodmwcli.UpdateGood(ctx, &appgoodmgrpb.AppGoodReq{
				ID:                &val.ID,
				DailyRewardAmount: &dailyReward,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
