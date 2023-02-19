package benefit

import (
	"context"
	"fmt"

	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"

	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/appgood"
	appgoodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/appgood"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/appgood"

	miningbookkeepingmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/mining/bookkeeping"
	miningbookkeepingmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/mining/bookkeeping"

	ledgerv2mwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger/v2"
	ledgerdetailmgrpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
)

func (st *State) BookKeeping(ctx context.Context, good *Good) error { //nolint
	totalReward, err := decimal.NewFromString(good.LastBenefitAmount)
	if err != nil {
		return err
	}
	if totalReward.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid reward")
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
	totalOrderUnits := decimal.NewFromInt(0)

	for _, ord := range ords {
		appIDs = append(appIDs, ord.AppID)
		units, err := decimal.NewFromString(ord.Units)
		if err != nil {
			return err
		}
		totalOrderUnits = totalOrderUnits.Add(units)
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

	appUnits := map[string]string{}
	for _, ord := range ords {
		_, ok := goodMap[ord.AppID]
		if !ok {
			continue
		}

		appUnits[ord.AppID] += ord.Units
	}

	appUnitRewards := map[string]decimal.Decimal{}
	totalUserReward := decimal.NewFromInt(0)
	totalFeeAmount := decimal.NewFromInt(0)
	goodTotal, err := decimal.NewFromString(good.GoodTotal)
	if err != nil {
		return err
	}

	userRewardAmount := totalReward.
		Mul(totalOrderUnits).
		Div(goodTotal)
	totalUnsoldReward := totalReward.
		Sub(userRewardAmount)

	for appID, unitsStr := range appUnits {
		ag, ok := goodMap[appID]
		if !ok {
			continue
		}
		units, err := decimal.NewFromString(unitsStr)
		if err != nil {
			return err
		}
		reward := userRewardAmount.
			Mul(units).
			Div(totalOrderUnits)

		fee := reward.
			Mul(decimal.NewFromInt(int64(ag.TechnicalFeeRatio))).
			Div(decimal.NewFromInt(100))

		userReward := reward.Sub(fee)
		totalUserReward = totalUserReward.Add(userReward)
		totalFeeAmount = totalFeeAmount.Add(fee)

		appUnitRewards[appID] = userReward.Div(units)
	}

	logger.Sugar().Infow("BookKeeping",
		"GoodID", good.ID,
		"LastBenefitAt", good.LastBenefitAt,
		"LastBenefitAmount", good.LastBenefitAmount,
		"TotalOrderUnits", totalOrderUnits,
		"TotalOrders", len(ords),
		"TechniqueServiceFee", totalFeeAmount,
		"Unsold", totalUnsoldReward,
		"UserReward", totalUserReward,
	)

	err = miningbookkeepingmwcli.BookKeeping(
		ctx,
		&miningbookkeepingmwpb.BookKeepingRequest{
			GoodID:                    good.ID,
			CoinTypeID:                good.CoinTypeID,
			TotalAmount:               totalReward.String(),
			UnsoldAmount:              totalUnsoldReward.String(),
			TechniqueServiceFeeAmount: totalFeeAmount.String(),
			BenefitDate:               good.LastBenefitAt,
		})
	if err != nil {
		return err
	}

	details := []*ledgerdetailmgrpb.DetailReq{}

	ioType := ledgerdetailmgrpb.IOType_Incoming
	ioSubType := ledgerdetailmgrpb.IOSubType_MiningBenefit

	for _, ord := range ords {
		unitReward, ok := appUnitRewards[ord.AppID]
		if !ok {
			continue
		}
		if unitReward.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}

		ioExtra := fmt.Sprintf(`{"GoodID":"%v","OrderID":"%v","BenefitDate":"%v"}`,
			good.ID, ord.ID, good.LastBenefitAt)
		units, err := decimal.NewFromString(ord.Units)
		if err != nil {
			return err
		}
		amountS := unitReward.
			Mul(units).
			String()

		details = append(details, &ledgerdetailmgrpb.DetailReq{
			AppID:      &ord.AppID,
			UserID:     &ord.UserID,
			CoinTypeID: &good.CoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &amountS,
			IOExtra:    &ioExtra,
		})
	}

	state := goodmgrpb.BenefitState_BenefitWait
	req := &goodmwpb.GoodReq{
		ID:           &good.ID,
		BenefitState: &state,
	}
	_, err = goodmwcli.UpdateGood(ctx, req)
	if err != nil {
		return err
	}

	if len(details) > 0 {
		err = ledgerv2mwcli.BookKeeping(ctx, details)
		if err != nil {
			return err
		}
	}

	return nil
}
