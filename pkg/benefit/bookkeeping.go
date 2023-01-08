package benefit

import (
	"context"
	"fmt"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/appgood"
	appgoodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/appgood"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/appgood"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
)

func (st *State) BookKeeping(ctx context.Context, good *Good) error {
	if len(good.BenefitTIDs) == 0 {
		return fmt.Errorf("invalid benefit txs")
	}

	txs, _, err := txmwcli.GetTxs(ctx, &txmgrpb.Conds{
		IDs: &commonpb.StringSliceVal{
			Op:    cruder.IN,
			Value: good.BenefitTIDs,
		},
	}, int32(0), int32(len(good.BenefitTIDs)))
	if err != nil {
		return err
	}

	totalReward := decimal.NewFromInt(0)

	for _, tx := range txs {
		switch tx.Type {
		case txmgrpb.TxType_TxPlatformBenefit:
		case txmgrpb.TxType_TxUserBenefit:
		default:
			return fmt.Errorf("invalid tx type")
		}

		amount, err := decimal.NewFromString(tx.Amount)
		if err != nil {
			return err
		}
		totalReward = totalReward.Add(amount)
	}

	ords := []*ordermwpb.Order{}
	offset := int32(0)
	limit := int32(100)

	for {
		_ords, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: good.ID,
			},
			LastBenefitAt: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: good.LastBenefitAt,
			},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(_ords) == 0 {
			break
		}

		ords = append(ords, _ords...)

		offset += limit
	}

	appIDs := []string{}
	totalOrderUnits := uint32(0)

	for _, ord := range ords {
		appIDs = append(appIDs, ord.AppID)
		totalOrderUnits += ord.Units
	}

	ags, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmgrpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: good.ID,
		},
		AppIDs: &commonpb.StringSliceVal{
			Op:    cruder.IN,
			Value: appIDs,
		},
	}, int32(0), int32(len(appIDs)))
	if err != nil {
		return err
	}

	goodMap := map[string]*appgoodmwpb.Good{}
	for _, ag := range ags {
		goodMap[ag.AppID] = ag
	}

	appUnits := map[string]uint32{}
	for _, ord := range ords {
		_, ok := goodMap[ord.AppID]
		if !ok {
			continue
		}

		appUnits[ord.AppID] += ord.Units
	}

	appUnitRewards := map[string]decimal.Decimal{}
	for appID, units := range appUnits {
		ag, ok := goodMap[appID]
		if !ok {
			continue
		}

		reward := totalReward.
			Mul(decimal.NewFromInt(int64(units))).
			Div(decimal.NewFromInt(int64(totalOrderUnits)))

		fee := reward.
			Mul(decimal.NewFromInt(int64(ag.TechnicalFeeRatio))).
			Div(decimal.NewFromInt(100))

		userReward := reward.Sub(fee)
		appUnitRewards[appID] = userReward.Div(decimal.NewFromInt(int64(units)))
	}

	// TODO: bookkeeping good profit
	// TODO: bookkeeping user profit

	return nil
}
