package executor

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	goodstmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/good/ledger/statement"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	goodstmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/good/ledger/statement"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/bookkeeping/good/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent              chan interface{}
	notif                   chan interface{}
	done                    chan interface{}
	totalRewardAmount       decimal.Decimal
	totalUnits              decimal.Decimal
	totalOrderUnits         decimal.Decimal
	appOrderUnits           map[string]map[string]decimal.Decimal
	goods                   map[string]map[string]*appgoodmwpb.Good
	goodCreatedAt           uint32
	techniqueFeeGood        *goodmwpb.Good
	userRewardAmount        decimal.Decimal
	unsoldRewardAmount      decimal.Decimal
	totalBenefitOrderUnits  decimal.Decimal
	totalTechniqueFeeAmount decimal.Decimal
	statementExist          bool
}

func (h *goodHandler) getOrderUnits(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
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
			h.totalBenefitOrderUnits = h.totalBenefitOrderUnits.Add(units)
			appGoodUnits[order.AppGoodID] = appGoodUnits[order.AppGoodID].Add(units)
			h.appOrderUnits[order.AppID] = appGoodUnits
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) getAppGoods(ctx context.Context) error {
	good, err := goodmwcli.GetGood(ctx, h.EntID)
	if err != nil {
		return err
	}

	h.goodCreatedAt = good.CreatedAt

	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			_goods, ok := h.goods[good.AppID]
			if !ok {
				_goods = map[string]*appgoodmwpb.Good{}
			}
			_goods[good.EntID] = good
			h.goods[good.AppID] = _goods
		}
		offset += limit
	}
	return nil
}

func (h *goodHandler) getTechniqueFeeGood(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit
	requireds := []*requiredmwpb.Required{}

	for {
		_requireds, _, err := requiredmwcli.GetRequireds(ctx, &requiredmwpb.Conds{
			MainGoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(_requireds) == 0 {
			break
		}
		requireds = append(requireds, _requireds...)
		offset += limit
	}

	offset = 0
	requiredGoodIDs := []string{}
	for _, required := range requireds {
		requiredGoodIDs = append(requiredGoodIDs, required.RequiredGoodID)
	}

	for {
		goods, _, err := goodmwcli.GetGoods(ctx, &goodmwpb.Conds{
			EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: requiredGoodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			break
		}
		for _, good := range goods {
			if good.GoodType != goodtypes.GoodType_TechniqueServiceFee {
				continue
			}
			if h.techniqueFeeGood != nil {
				return fmt.Errorf("too many techniquefeegood")
			}
			h.techniqueFeeGood = good
		}
		offset += limit
	}

	return nil
}

func (h *goodHandler) calculateTechniqueFeeLegacy() {
	for appID, appGoodUnits := range h.appOrderUnits {
		goods, ok := h.goods[appID]
		if !ok {
			continue
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
			h.totalTechniqueFeeAmount = h.totalTechniqueFeeAmount.
				Add(techniqueFee)
		}
	}
}

func (h *goodHandler) calculateTechniqueFee(ctx context.Context) error {
	if h.totalBenefitOrderUnits.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}
	if h.userRewardAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	const legacyTechniqueFeeTimestamp = 1704009402
	if h.goodCreatedAt <= legacyTechniqueFeeTimestamp {
		h.calculateTechniqueFeeLegacy()
		return nil
	}
	if err := h.getTechniqueFeeGood(ctx); err != nil {
		return err
	}
	if h.techniqueFeeGood == nil {
		return nil
	}
	if h.techniqueFeeGood.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfit {
		return nil
	}

	feePercent, err := decimal.NewFromString(h.techniqueFeeGood.UnitPrice)
	if err != nil {
		return err
	}

	h.totalTechniqueFeeAmount = h.userRewardAmount.Mul(feePercent).Div(decimal.NewFromInt(100))
	return nil
}

func (h *goodHandler) checkGoodStatement(ctx context.Context) (bool, error) {
	exist, err := goodstmwcli.ExistGoodStatementConds(ctx, &goodstmwpb.Conds{
		GoodID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
		BenefitDate: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
	})
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	h.statementExist = true
	return true, nil
}

//nolint:gocritic
func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Good", h.Good,
			"TotalRewardAmount", h.totalRewardAmount,
			"UserRewardAmount", h.userRewardAmount,
			"UnsoldRewardAmount", h.unsoldRewardAmount,
			"TotalTechniqueFeeAmount", h.totalTechniqueFeeAmount,
			"AppOrderUnits", h.appOrderUnits,
			"StatementExist", h.statementExist,
			"Error", *err,
		)
	}
	persistentGood := &types.PersistentGood{
		Good:               h.Good,
		TotalRewardAmount:  h.totalRewardAmount.String(),
		UnsoldRewardAmount: h.unsoldRewardAmount.String(),
		TechniqueFeeAmount: h.totalTechniqueFeeAmount.String(),
		StatementExist:     h.statementExist,
		Error:              *err,
	}

	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}

	persistentGood.BenefitResult = basetypes.Result_Fail
	persistentGood.BenefitMessage = (*err).Error()

	asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint:gocritic
func (h *goodHandler) exec(ctx context.Context) error {
	h.goods = map[string]map[string]*appgoodmwpb.Good{}
	h.appOrderUnits = map[string]map[string]decimal.Decimal{}
	var err error

	defer h.final(ctx, &err)

	h.totalRewardAmount, err = decimal.NewFromString(h.LastRewardAmount)
	if err != nil {
		return err
	}
	if exist, err := h.checkGoodStatement(ctx); err != nil || exist {
		return err
	}
	if err = h.getAppGoods(ctx); err != nil {
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
	if err := h.calculateTechniqueFee(ctx); err != nil {
		return err
	}

	return nil
}
