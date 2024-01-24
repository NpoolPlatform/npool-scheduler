package executor

import (
	"context"
	"fmt"
	"sort"
	"time"

	currencymwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency"
	coinusedformwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/usedfor"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	chaintypes "github.com/NpoolPlatform/message/npool/basetypes/chain/v1"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	currencymwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency"
	coinusedformwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/usedfor"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/renew/check/types"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*ordermwpb.Order
	persistent              chan interface{}
	done                    chan interface{}
	notif                   chan interface{}
	requireds               []*requiredmwpb.Required
	mainAppGood             *appgoodmwpb.Good
	electricityFeeAppGood   *appgoodmwpb.Good
	techniqueFeeAppGood     *appgoodmwpb.Good
	newRenewState           ordertypes.OrderRenewState
	childOrders             []*ordermwpb.Order
	techniqueFeeDuration    uint32
	electricityFeeDuration  uint32
	electricityFeeEndAt     uint32
	techniqueFeeEndAt       uint32
	userNotifText           string
	nextNotifyInterval      uint32
	insufficientBalance     bool
	feeDeductionCoins       []*coinusedformwpb.CoinUsedFor
	feeDeductions           []*types.FeeDeduction
	ledgers                 []*ledgermwpb.Ledger
	currencies              map[string]*currencymwpb.Currency
	checkElectricityFee     bool
	checkTechniqueFee       bool
	electricityFeeUSDAmount decimal.Decimal
	techniqueFeeUSDAmount   decimal.Decimal
	notifiable              bool
}

func (h *orderHandler) getRequireds(ctx context.Context) error {
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

func (h *orderHandler) getAppGoods(ctx context.Context) error {
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
				h.electricityFeeAppGood = appGood
			case goodtypes.GoodType_TechniqueServiceFee:
				h.techniqueFeeAppGood = appGood
			}
			if appGood.EntID == h.AppGoodID {
				h.mainAppGood = appGood
			}
		}
		offset += limit
	}

	if h.mainAppGood == nil {
		return fmt.Errorf("invalid mainappgood")
	}

	return nil
}

func (h *orderHandler) renewGoodExist() (bool, error) {
	if h.mainAppGood.PackageWithRequireds {
		return false, nil
	}
	return h.techniqueFeeAppGood != nil || h.electricityFeeAppGood != nil, nil
}

func (h *orderHandler) getRenewableOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	appGoodIDs := []string{}
	if h.electricityFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.electricityFeeAppGood.EntID)
	}
	if h.techniqueFeeAppGood != nil {
		appGoodIDs = append(appGoodIDs, h.techniqueFeeAppGood.EntID)
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

	if h.electricityFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.electricityFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.electricityFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	if h.techniqueFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.techniqueFeeAppGood.EntID {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.techniqueFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
		}
	}

	return nil
}

func (h *orderHandler) renewable() bool {
	now := uint32(time.Now().Unix())
	if h.StartAt >= now || h.EndAt <= now {
		return false
	}

	orderElapsed := now - h.StartAt
	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour
	feeElapsed := orderElapsed - outOfGas - compensate

	if h.electricityFeeAppGood != nil {
		if h.electricityFeeDuration <= feeElapsed {
			h.nextNotifyInterval = timedef.SecondsPerHour
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*1 {
			h.nextNotifyInterval = timedef.SecondsPerHour
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*3 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 3
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*6 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*12 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*18 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
			// TODO: create electricity fee renew order
		} else if h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*24 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
		}
		h.electricityFeeEndAt = h.electricityFeeDuration + outOfGas + compensate
		h.notifiable = h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
		h.checkElectricityFee = h.electricityFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}
	if h.techniqueFeeAppGood != nil && h.techniqueFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByCash {
		if h.techniqueFeeDuration <= feeElapsed {
			h.nextNotifyInterval = timedef.SecondsPerHour
		} else if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*1 {
			h.nextNotifyInterval = timedef.SecondsPerHour
		} else if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*3 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 3
		} else if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*6 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
		} else if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*12 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
		} else if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*18 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
			// TODO: create technique fee renew order
		} else if h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*24 {
			h.nextNotifyInterval = timedef.SecondsPerHour * 6
		}
		h.techniqueFeeEndAt = h.techniqueFeeDuration + outOfGas + compensate
		h.notifiable = h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
		h.checkTechniqueFee = h.techniqueFeeDuration <= feeElapsed+timedef.SecondsPerHour*24
	}

	return false
}

func (h *orderHandler) getFeeDeductionCoins(ctx context.Context) error {
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
		h.feeDeductionCoins = append(h.feeDeductionCoins, coinUsedFors...)
		offset += limit
	}

	return nil
}

func (h *orderHandler) getUserLedgers(ctx context.Context) error {
	if len(h.feeDeductionCoins) == 0 {
		return nil
	}

	coinTypeIDs := []string{}
	for _, coin := range h.feeDeductionCoins {
		coinTypeIDs = append(coinTypeIDs, coin.CoinTypeID)
	}

	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		ledgers, _, err := ledgermwcli.GetLedgers(ctx, &ledgermwpb.Conds{
			CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(ledgers) == 0 {
			break
		}
		h.ledgers = append(h.ledgers, ledgers...)
		offset += limit
	}

	return nil
}

func (h *orderHandler) getCoinUSDCurrency(ctx context.Context) error {
	if len(h.feeDeductionCoins) == 0 {
		return nil
	}

	coinTypeIDs := []string{}
	for _, coin := range h.feeDeductionCoins {
		coinTypeIDs = append(coinTypeIDs, coin.CoinTypeID)
	}

	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		currencies, _, err := currencymwcli.GetCurrencies(ctx, &currencymwpb.Conds{
			CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(currencies) == 0 {
			break
		}
		for _, currency := range currencies {
			h.currencies[currency.CoinTypeID] = currency
		}
		offset += limit
	}

	return nil
}

func (h *orderHandler) calculateFeeUSDAmount() error {
	orderUnits, err := decimal.NewFromString(h.Units)
	if err != nil {
		return err
	}

	if h.checkElectricityFee {
		unitPrice, err := decimal.NewFromString(h.electricityFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}
		durations := 1 //nolint
		switch h.electricityFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= timedef.HoursPerDay * 3
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
		case goodtypes.GoodDurationType_GoodDurationByMonth:
		case goodtypes.GoodDurationType_GoodDurationByYear:
		}
		h.electricityFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
	}

	if h.checkTechniqueFee {
		unitPrice, err := decimal.NewFromString(h.techniqueFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}
		durations := 1 //nolint
		switch h.techniqueFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= timedef.HoursPerDay * 3
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
		case goodtypes.GoodDurationType_GoodDurationByMonth:
		case goodtypes.GoodDurationType_GoodDurationByYear:
		}
		h.techniqueFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
	}

	return nil
}

func (h *orderHandler) calculateFeeDeduction() error {
	feeUSDAmount := h.electricityFeeUSDAmount.Add(h.techniqueFeeUSDAmount)
	if feeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}
	for _, ledger := range h.ledgers {
		currency, ok := h.currencies[ledger.CoinTypeID]
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
			h.feeDeductions = append(h.feeDeductions, &types.FeeDeduction{
				CoinName:    currency.CoinName,
				CoinUnit:    currency.CoinUnit,
				USDCurrency: currency.MarketValueLow,
				Amount:      feeCoinAmount.String(),
			})
			return nil
		}
		h.feeDeductions = append(h.feeDeductions, &types.FeeDeduction{
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

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Order", h.Order,
			"NewRenewState", h.newRenewState,
			"Error", *err,
		)
	}
	persistentOrder := &types.PersistentOrder{
		Order:         h.Order,
		NewRenewState: h.newRenewState,
	}
	if *err != nil {
		asyncfeed.AsyncFeed(ctx, h.Order, h.notif)
	}
	if h.newRenewState != h.RenewState {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, h.Order, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	h.newRenewState = h.RenewState
	h.currencies = map[string]*currencymwpb.Currency{}

	var err error
	var yes bool
	defer h.final(ctx, &err)

	if err = h.getRequireds(ctx); err != nil {
		return err
	}
	if err := h.getAppGoods(ctx); err != nil {
		return err
	}
	if yes, err = h.renewGoodExist(); err != nil || !yes {
		return err
	}
	if err = h.getRenewableOrders(ctx); err != nil {
		return err
	}
	if yes = h.renewable(); !yes {
		return err
	}
	if err = h.getFeeDeductionCoins(ctx); err != nil {
		return err
	}
	if err = h.getUserLedgers(ctx); err != nil {
		return err
	}
	if err = h.getCoinUSDCurrency(ctx); err != nil {
		return err
	}
	if err = h.calculateFeeUSDAmount(); err != nil {
		return err
	}
	if err = h.calculateFeeDeduction(); err != nil {
		return err
	}
	return nil
}
