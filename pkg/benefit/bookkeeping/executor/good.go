package executor

import (
	"context"
	"fmt"

	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	// goodstmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/good/ledger/statement"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	// goodstmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/good/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	retry1 "github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent              chan interface{}
	notif                   chan interface{}
	retry                   chan interface{}
	coin                    *coinmwpb.Coin
	totalRewardAmount       decimal.Decimal
	totalUnits              decimal.Decimal
	totalOrderUnits         decimal.Decimal
	appOrderUnits           map[string]map[string]decimal.Decimal
	goods                   map[string]map[string]*appgoodmwpb.Good
	userRewardAmount        decimal.Decimal
	unsoldRewardAmount      decimal.Decimal
	appGoodUnitRewards      map[string]map[string]decimal.Decimal
	totalUserRewardAmount   decimal.Decimal
	totalTechniqueFeeAmount decimal.Decimal
	orderRewards            []*types.OrderReward
	statementExist          bool
}

func (h *goodHandler) getOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		}, offset, limit)
		if err != nil {
			return err
		}
		for _, order := range orders {
			units, err := decimal.NewFromString(order.Units)
			if err != nil {
				return err
			}
			h.totalOrderUnits = h.totalOrderUnits.Add(units)
			appGoodUnits, ok := h.appOrderUnits[order.AppID]
			if !ok {
				appGoodUnits = map[string]decimal.Decimal{
					order.AppGoodID: decimal.NewFromInt(0),
				}
			}
			appGoodUnits[order.AppGoodID] = appGoodUnits[order.AppGoodID].Add(units)
			h.appOrderUnits[order.AppID] = appGoodUnits
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) getAppGoods(ctx context.Context) error {
	appIDs := []string{}
	appGoodIDs := []string{}
	for appID, appGoodUnits := range h.appOrderUnits {
		appIDs = append(appIDs, appID)
		for appGoodID, _ := range appGoodUnits {
			appGoodIDs = append(appGoodIDs, appGoodID)
		}
	}
	goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
		AppIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: appIDs},
		GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
		IDs:    &basetypes.StringSliceVal{Op: cruder.IN, Value: appGoodIDs},
	}, int32(0), int32(len(appGoodIDs)))
	if err != nil {
		return err
	}
	for _, good := range goods {
		appGoods, ok := h.goods[good.AppID]
		if !ok {
			appGoods = map[string]*appgoodmwpb.Good{}
		}
		appGoods[good.ID] = good
		h.goods[good.AppID] = appGoods
	}
	return nil
}

func (h *goodHandler) calculateUnitReward(ctx context.Context) error {
	for appID, appGoodUnits := range h.appOrderUnits {
		goods, ok := h.goods[appID]
		if !ok {
			continue
		}
		unitRewards, ok := h.appGoodUnitRewards[appID]
		if !ok {
			unitRewards = map[string]decimal.Decimal{}
		}
		for appGoodID, units := range appGoodUnits {
			good, ok := goods[appGoodID]
			if !ok {
				continue
			}
			userRewardAmount := h.userRewardAmount.
				Mul(units).
				Div(h.totalOrderUnits)
			techniqueFee := userRewardAmount.
				Mul(decimal.RequireFromString(good.TechnicalFeeRatio)).
				Div(decimal.NewFromInt(100))
			unitRewards[appGoodID] = userRewardAmount.
				Sub(techniqueFee).
				Div(units)
			h.totalUserRewardAmount = h.totalUserRewardAmount.
				Add(userRewardAmount).
				Sub(techniqueFee)
			h.totalTechniqueFeeAmount = h.totalTechniqueFeeAmount.
				Add(techniqueFee)
		}
		h.appGoodUnitRewards[appID] = unitRewards
	}
	return nil
}

func (h *goodHandler) calculateOrderReward(order *ordermwpb.Order) error {
	unitRewards, ok := h.appGoodUnitRewards[order.AppID]
	if !ok {
		return nil
	}
	unitReward, ok := unitRewards[order.AppGoodID]
	if !ok {
		return nil
	}
	ioExtra := fmt.Sprintf(
		`{"GoodID":"%v","OrderID":"%v","Units":"%v","BenefitDate":"%v"}`,
		h.ID,
		order.ID,
		order.Units,
		h.LastRewardAt,
	)
	units, err := decimal.NewFromString(order.Units)
	if err != nil {
		return err
	}
	amount := unitReward.Mul(units)
	h.orderRewards = append(h.orderRewards, &types.OrderReward{
		AppID:  order.AppID,
		UserID: order.UserID,
		Amount: amount.String(),
		Extra:  ioExtra,
	})
	return nil
}

func (h *goodHandler) calculateOrderRewards(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}

		for _, order := range orders {
			if err := h.calculateOrderReward(order); err != nil {
				return err
			}
		}

		offset += limit
	}
}

func (h *goodHandler) checkGoodStatement(ctx context.Context) (bool, error) {
	/*
		exist, err := goodstmwcli.ExistGoodStatement(ctx, &goodstmwpb.Conds{
			GoodID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
			BenefitDate: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		})
		if err != nil {
			return false, err
		}
		if !exist {
			return false, nil
		}
		h.statementExist = true
	*/
	return false, nil
}

func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err == nil {
		return
	}

	persistentGood := &types.PersistentGood{
		Good:               h.Good,
		TotalRewardAmount:  h.totalRewardAmount.String(),
		UnsoldRewardAmount: h.unsoldRewardAmount.String(),
		TechniqueFeeAmount: h.totalTechniqueFeeAmount.String(),
		UserRewardAmount:   h.totalUserRewardAmount.String(),
		StatementExist:     h.statementExist,
		OrderRewards:       h.orderRewards,
		Error:              *err,
	}

	if *err == nil {
		h.persistent <- persistentGood
	} else {
		retry1.Retry(ctx, h.Good, h.retry)
		h.notif <- persistentGood
	}
}

func (h *goodHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	h.totalRewardAmount, err = decimal.NewFromString(h.LastRewardAmount)
	if err != nil {
		return err
	}
	if exist, err := h.checkGoodStatement(ctx); err != nil || exist {
		return err
	}
	h.totalUnits, err = decimal.NewFromString(h.GoodTotal)
	if err != nil {
		return err
	}
	if err = h.getOrderUnits(ctx); err != nil {
		return err
	}
	h.userRewardAmount = h.totalRewardAmount.
		Mul(h.totalOrderUnits).
		Div(h.totalUnits)
	h.unsoldRewardAmount = h.totalRewardAmount.
		Sub(h.userRewardAmount)
	if err = h.calculateUnitReward(ctx); err != nil {
		return err
	}
	if err = h.calculateOrderRewards(ctx); err != nil {
		return err
	}

	return nil
}
