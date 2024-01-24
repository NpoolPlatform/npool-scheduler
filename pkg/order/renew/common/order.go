package executor

import (
	"context"
	"fmt"
	"sort"
	"time"

	currencymwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency"
	coinusedformwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/usedfor"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	chaintypes "github.com/NpoolPlatform/message/npool/basetypes/chain/v1"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	currencymwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency"
	coinusedformwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/usedfor"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/notify/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type OrderHandler struct {
	*ordermwpb.Order
	requireds               []*requiredmwpb.Required
	MainAppGood             *appgoodmwpb.Good
	ElectricityFeeAppGood   *appgoodmwpb.Good
	TechniqueFeeAppGood     *appgoodmwpb.Good
	childOrders             []*ordermwpb.Order
	TechniqueFeeDuration    uint32
	TechniqueFeeEndAt       uint32
	ElectricityFeeDuration  uint32
	ElectricityFeeEndAt     uint32
	FeeDeductionCoins       []*coinusedformwpb.CoinUsedFor
	FeeDeductions           []*types.FeeDeduction
	UserLedgers             []*ledgermwpb.Ledger
	Currencies              map[string]*currencymwpb.Currency
	ElectricityFeeUSDAmount decimal.Decimal
	TechniqueFeeUSDAmount   decimal.Decimal
	CheckElectricityFee     bool
	CheckTechniqueFee       bool
}

func (h *OrderHandler) GetRequireds(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		requireds, _, err := requiredmwcli.GetRequireds(ctx, &requiredmwpb.Conds{
			MainGoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(requireds) == 0 {
			break
		}
		h.requireds = append(h.requireds, requireds...)
		offset += limit
	}
	return nil
}

func (h *OrderHandler) GetAppGoods(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	goodIDs := []string{h.GoodID}
	for _, required := range h.requireds {
		goodIDs = append(goodIDs, required.RequiredGoodID)
	}

	for {
		appGoods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			AppID:   &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
			GoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: goodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(appGoods) == 0 {
			break
		}
		for _, appGood := range appGoods {
			switch appGood.GoodType {
			case goodtypes.GoodType_ElectricityFee:
				h.ElectricityFeeAppGood = appGood
			case goodtypes.GoodType_TechniqueServiceFee:
				h.TechniqueFeeAppGood = appGood
			}
			if appGood.EntID == h.AppGoodID {
				h.MainAppGood = appGood
			}
		}
		offset += limit
	}

	if h.MainAppGood == nil {
		return fmt.Errorf("invalid mainappgood")
	}

	return nil
}

func (h *OrderHandler) RenewGoodExist() (bool, error) {
	if h.MainAppGood.PackageWithRequireds {
		return false, nil
	}
	return h.TechniqueFeeAppGood != nil || h.ElectricityFeeAppGood != nil, nil
}

func (h *OrderHandler) GetRenewableOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	appGoodIDs := []string{}
	if h.ElectricityFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.ElectricityFeeAppGood.EntID)
	}
	if h.TechniqueFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.TechniqueFeeAppGood.EntID)
	}

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			ParentOrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.EntID},
			AppGoodIDs:    &basetypes.StringSliceVal{Op: cruder.IN, Value: appGoodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}
		h.childOrders = append(h.childOrders, orders...)
		offset += limit
	}

	sort.Slice(h.childOrders, func(i, j int) bool {
		return h.childOrders[i].StartAt < h.childOrders[j].StartAt
	})

	if h.ElectricityFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.ElectricityFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.ElectricityFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	if h.TechniqueFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.TechniqueFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.TechniqueFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour
	ignoredSeconds := outOfGas + compensate

	h.TechniqueFeeEndAt = h.StartAt + h.TechniqueFeeDuration + ignoredSeconds
	h.ElectricityFeeEndAt = h.StartAt + h.ElectricityFeeDuration + ignoredSeconds

	now := uint32(time.Now().Unix())
	if h.ElectricityFeeAppGood != nil {
		h.CheckElectricityFee = h.StartAt+h.ElectricityFeeDuration+ignoredSeconds < now+timedef.SecondsPerHour*24
	}
	if h.TechniqueFeeAppGood != nil {
		h.CheckTechniqueFee = h.StartAt+h.TechniqueFeeDuration+ignoredSeconds < now+timedef.SecondsPerHour*24
	}

	return nil
}

func (h *OrderHandler) GetFeeDeductionCoins(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		coinUsedFors, _, err := coinusedformwcli.GetCoinUsedFors(ctx, &coinusedformwpb.Conds{
			UsedFor: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(chaintypes.CoinUsedFor_CoinUsedForGoodFee)},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(coinUsedFors) == 0 {
			break
		}
		h.FeeDeductionCoins = append(h.FeeDeductionCoins, coinUsedFors...)
		offset += limit
	}

	if len(h.FeeDeductionCoins) == 0 {
		return fmt.Errorf("invalid feedudectioncoins")
	}

	return nil
}

func (h *OrderHandler) GetUserLedgers(ctx context.Context) error {
	coinTypeIDs := []string{}
	for _, coin := range h.FeeDeductionCoins {
		coinTypeIDs = append(coinTypeIDs, coin.CoinTypeID)
	}

	ledgers, _, err := ledgermwcli.GetLedgers(ctx, &ledgermwpb.Conds{
		AppID:       &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		UserID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.UserID},
		CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
	}, 0, int32(len(coinTypeIDs)))
	if err != nil {
		return err
	}
	h.UserLedgers = append(h.UserLedgers, ledgers...)

	return nil
}

func (h *OrderHandler) GetCoinUSDCurrency(ctx context.Context) error {
	coinTypeIDs := []string{}
	for _, coin := range h.FeeDeductionCoins {
		coinTypeIDs = append(coinTypeIDs, coin.CoinTypeID)
	}

	currencies, _, err := currencymwcli.GetCurrencies(ctx, &currencymwpb.Conds{
		CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
	}, 0, int32(len(coinTypeIDs)))
	if err != nil {
		return err
	}
	for _, currency := range currencies {
		h.Currencies[currency.CoinTypeID] = currency
	}

	return nil
}

func (h *OrderHandler) CalculateFeeUSDAmount() error {
	orderUnits, err := decimal.NewFromString(h.Units)
	if err != nil {
		return err
	}

	if h.CheckElectricityFee {
		unitPrice, err := decimal.NewFromString(h.ElectricityFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}
		durations := 1 //nolint
		switch h.ElectricityFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= timedef.HoursPerDay * 3
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
		case goodtypes.GoodDurationType_GoodDurationByMonth:
		case goodtypes.GoodDurationType_GoodDurationByYear:
		}
		h.ElectricityFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
	}

	if h.CheckTechniqueFee {
		unitPrice, err := decimal.NewFromString(h.TechniqueFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}
		durations := 1 //nolint
		switch h.TechniqueFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= timedef.HoursPerDay * 3
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
		case goodtypes.GoodDurationType_GoodDurationByMonth:
		case goodtypes.GoodDurationType_GoodDurationByYear:
		}
		h.TechniqueFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
	}

	return nil
}

func (h *OrderHandler) CalculateFeeDeduction() error {
	feeUSDAmount := h.ElectricityFeeUSDAmount.Add(h.TechniqueFeeUSDAmount)
	if feeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}
	for _, ledger := range h.UserLedgers {
		currency, ok := h.Currencies[ledger.CoinTypeID]
		if !ok {
			return fmt.Errorf("invalid coinusdcurrency")
		}
		currencyValue, err := decimal.NewFromString(currency.MarketValueLow)
		if err != nil {
			return err
		}
		if currencyValue.Cmp(decimal.NewFromInt(0)) <= 0 {
			return fmt.Errorf("invalid coinusdcurrency")
		}
		spendable, err := decimal.NewFromString(ledger.Spendable)
		if err != nil {
			return err
		}
		feeCoinAmount := feeUSDAmount.Div(currencyValue)
		if spendable.Cmp(feeCoinAmount) >= 0 {
			h.FeeDeductions = append(h.FeeDeductions, &types.FeeDeduction{
				CoinTypeID:  ledger.CoinTypeID,
				CoinName:    currency.CoinName,
				CoinUnit:    currency.CoinUnit,
				USDCurrency: currency.MarketValueLow,
				Amount:      feeCoinAmount.String(),
			})
			return nil
		}
		h.FeeDeductions = append(h.FeeDeductions, &types.FeeDeduction{
			CoinTypeID:  ledger.CoinTypeID,
			CoinName:    currency.CoinName,
			CoinUnit:    currency.CoinUnit,
			USDCurrency: currency.MarketValueLow,
			Amount:      spendable.String(),
		})
		spendableUSD := spendable.Mul(currencyValue)
		feeUSDAmount = feeUSDAmount.Sub(spendableUSD)
	}
	return fmt.Errorf("insufficient balance")
}
