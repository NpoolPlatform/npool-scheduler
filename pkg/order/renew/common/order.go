package executor

import (
	"context"
	"fmt"
	"sort"
	"time"

	appcoinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/app/coin"
	currencymwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency"
	coinusedformwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/usedfor"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	requiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/good/required"
	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	chaintypes "github.com/NpoolPlatform/message/npool/basetypes/chain/v1"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appcoinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/app/coin"
	currencymwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency"
	coinusedformwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/usedfor"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	requiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/required"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	orderrenewpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order/renew"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	"github.com/shopspring/decimal"
)

type OrderHandler struct {
	*ordermwpb.Order
	requireds                      []*requiredmwpb.Required
	MainAppGood                    *appgoodmwpb.Good
	ElectricityFeeAppGood          *appgoodmwpb.Good
	TechniqueFeeAppGood            *appgoodmwpb.Good
	childOrders                    []*ordermwpb.Order
	TechniqueFeeDuration           uint32
	TechniqueFeeExtendDuration     uint32
	TechniqueFeeExtendSeconds      uint32
	TechniqueFeeEndAt              uint32
	ExistUnpaidTechniqueFeeOrder   bool
	ElectricityFeeDuration         uint32
	ElectricityFeeExtendDuration   uint32
	ElectricityFeeExtendSeconds    uint32
	ElectricityFeeEndAt            uint32
	ExistUnpaidElectricityFeeOrder bool
	DeductionCoins                 []*coinusedformwpb.CoinUsedFor
	DeductionAppCoins              map[string]*appcoinmwpb.Coin
	Deductions                     []*orderrenewpb.Deduction
	UserLedgers                    map[string]*ledgermwpb.Ledger
	Currencies                     map[string]*currencymwpb.Currency
	ElectricityFeeUSDAmount        decimal.Decimal
	TechniqueFeeUSDAmount          decimal.Decimal
	CheckElectricityFee            bool
	CheckTechniqueFee              bool
	InsufficientBalance            bool
	RenewInfos                     []*orderrenewpb.RenewInfo
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

// TODO: for some child goods which are suggested by us, we may also notify it to user when it's over
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

//nolint:gocognit,gocyclo
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

	// TODO: only check paid order. If unpaid order exist, we should just wait

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

	maxElectricityFeeEndAt := uint32(0)
	if h.ElectricityFeeAppGood != nil {
		for _, order := range h.childOrders {
			if order.AppGoodID == h.ElectricityFeeAppGood.EntID {
				switch order.PaymentState {
				case ordertypes.PaymentState_PaymentStateDone:
				case ordertypes.PaymentState_PaymentStateNoPayment:
				case ordertypes.PaymentState_PaymentStateWait:
					h.ExistUnpaidElectricityFeeOrder = true
					continue
				default:
					continue
				}
				if order.StartAt < maxElectricityFeeEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.ElectricityFeeDuration += order.EndAt - order.StartAt
				maxElectricityFeeEndAt = order.EndAt
			}
		}
	}

	maxTechniqueFeeEndAt := uint32(0)
	if h.TechniqueFeeAppGood != nil {
		for _, order := range h.childOrders {
			if order.AppGoodID == h.TechniqueFeeAppGood.EntID {
				switch order.PaymentState {
				case ordertypes.PaymentState_PaymentStateDone:
				case ordertypes.PaymentState_PaymentStateNoPayment:
				case ordertypes.PaymentState_PaymentStateWait:
					h.ExistUnpaidTechniqueFeeOrder = true
					continue
				default:
					continue
				}
				if order.StartAt < maxTechniqueFeeEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.TechniqueFeeDuration += order.EndAt - order.StartAt
				maxTechniqueFeeEndAt = order.EndAt
			}
		}
	}

	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour
	ignoredSeconds := outOfGas + compensate

	h.ElectricityFeeEndAt = h.StartAt + h.ElectricityFeeDuration + ignoredSeconds
	if h.ElectricityFeeEndAt < maxElectricityFeeEndAt {
		h.ElectricityFeeEndAt = maxElectricityFeeEndAt
	}
	h.TechniqueFeeEndAt = h.StartAt + h.TechniqueFeeDuration + ignoredSeconds
	if h.TechniqueFeeEndAt < maxTechniqueFeeEndAt {
		h.TechniqueFeeEndAt = maxTechniqueFeeEndAt
	}

	now := uint32(time.Now().Unix())
	const secondsBeforeFeeExhausted = timedef.SecondsPerHour * 24

	if h.ElectricityFeeAppGood != nil && h.ElectricityFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByProfit {
		h.ElectricityFeeEndAt = h.EndAt
	}
	if h.TechniqueFeeAppGood != nil && h.TechniqueFeeAppGood.SettlementType == goodtypes.GoodSettlementType_GoodSettledByProfit {
		h.TechniqueFeeEndAt = h.EndAt
	}

	if h.ElectricityFeeAppGood != nil && h.ElectricityFeeEndAt < h.EndAt {
		h.CheckElectricityFee = h.ElectricityFeeAppGood.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfit &&
			h.StartAt+h.ElectricityFeeDuration+ignoredSeconds < now+secondsBeforeFeeExhausted
	}
	if h.TechniqueFeeAppGood != nil && h.TechniqueFeeEndAt < h.EndAt {
		h.CheckTechniqueFee = h.TechniqueFeeAppGood.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfit &&
			h.StartAt+h.TechniqueFeeDuration+ignoredSeconds < now+secondsBeforeFeeExhausted
	}

	return nil
}

func (h *OrderHandler) GetDeductionCoins(ctx context.Context) error {
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
		h.DeductionCoins = append(h.DeductionCoins, coinUsedFors...)
		offset += limit
	}

	if len(h.DeductionCoins) == 0 {
		return fmt.Errorf("invalid feedudectioncoins")
	}
	sort.Slice(h.DeductionCoins, func(i, j int) bool {
		return h.DeductionCoins[i].Priority < h.DeductionCoins[j].Priority
	})

	return nil
}

func (h *OrderHandler) GetDeductionAppCoins(ctx context.Context) error {
	coinTypeIDs := []string{}
	for _, coin := range h.DeductionCoins {
		coinTypeIDs = append(coinTypeIDs, coin.CoinTypeID)
	}

	h.DeductionAppCoins = map[string]*appcoinmwpb.Coin{}

	coins, _, err := appcoinmwcli.GetCoins(ctx, &appcoinmwpb.Conds{
		AppID:       &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
	}, 0, int32(len(coinTypeIDs)))
	if err != nil {
		return err
	}
	for _, coin := range coins {
		h.DeductionAppCoins[coin.CoinTypeID] = coin
	}
	return nil
}

func (h *OrderHandler) GetUserLedgers(ctx context.Context) error {
	coinTypeIDs := []string{}
	for _, coin := range h.DeductionCoins {
		coinTypeIDs = append(coinTypeIDs, coin.CoinTypeID)
	}

	h.UserLedgers = map[string]*ledgermwpb.Ledger{}

	ledgers, _, err := ledgermwcli.GetLedgers(ctx, &ledgermwpb.Conds{
		AppID:       &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		UserID:      &basetypes.StringVal{Op: cruder.EQ, Value: h.UserID},
		CoinTypeIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
	}, 0, int32(len(coinTypeIDs)))
	if err != nil {
		return err
	}
	for _, ledger := range ledgers {
		h.UserLedgers[ledger.CoinTypeID] = ledger
	}

	return nil
}

func (h *OrderHandler) GetCoinUSDCurrency(ctx context.Context) error {
	coinTypeIDs := []string{}
	for _, coin := range h.DeductionCoins {
		coinTypeIDs = append(coinTypeIDs, coin.CoinTypeID)
	}

	h.Currencies = map[string]*currencymwpb.Currency{}

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

//nolint:gocognit
func (h *OrderHandler) CalculateUSDAmount() error {
	orderUnits, err := decimal.NewFromString(h.Units)
	if err != nil {
		return err
	}

	now := uint32(time.Now().Unix())
	remainSeconds := h.EndAt - now
	unitSeconds := timedef.HoursPerDay

	//nolint:dupl
	if h.CheckElectricityFee {
		unitPrice, err := decimal.NewFromString(h.ElectricityFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}
		durations := 1 //nolint
		switch h.ElectricityFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= 3 * timedef.HoursPerDay
			unitSeconds = timedef.SecondsPerHour
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
			unitSeconds = timedef.SecondsPerDay
		case goodtypes.GoodDurationType_GoodDurationByMonth:
			unitSeconds = timedef.SecondsPerMonth
		case goodtypes.GoodDurationType_GoodDurationByYear:
			unitSeconds = timedef.SecondsPerYear
		}

		seconds := uint32(durations * unitSeconds)
		if seconds > remainSeconds {
			seconds = remainSeconds
			durations = int(seconds) / unitSeconds
			if int(seconds)%unitSeconds != 0 {
				durations++
			}
		}

		h.ElectricityFeeExtendDuration = uint32(durations)
		h.ElectricityFeeExtendSeconds = seconds

		h.ElectricityFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
		h.RenewInfos = append(h.RenewInfos, &orderrenewpb.RenewInfo{
			AppGood:       h.ElectricityFeeAppGood,
			EndAt:         h.ElectricityFeeEndAt,
			RenewDuration: uint32(durations),
		})
	}

	//nolint:dupl
	if h.CheckTechniqueFee {
		unitPrice, err := decimal.NewFromString(h.TechniqueFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}
		durations := 1 //nolint
		switch h.TechniqueFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= 3 * timedef.HoursPerDay
			unitSeconds = timedef.SecondsPerHour
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
			unitSeconds = timedef.SecondsPerDay
		case goodtypes.GoodDurationType_GoodDurationByMonth:
			unitSeconds = timedef.SecondsPerMonth
		case goodtypes.GoodDurationType_GoodDurationByYear:
			unitSeconds = timedef.SecondsPerYear
		}

		seconds := uint32(durations * unitSeconds)
		if seconds > remainSeconds {
			seconds = remainSeconds
			durations = int(seconds) / unitSeconds
			if int(seconds)%unitSeconds != 0 {
				durations++
			}
		}

		h.TechniqueFeeExtendDuration = uint32(durations)
		h.TechniqueFeeExtendSeconds = seconds
		h.TechniqueFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
		h.RenewInfos = append(h.RenewInfos, &orderrenewpb.RenewInfo{
			AppGood:       h.TechniqueFeeAppGood,
			EndAt:         h.TechniqueFeeEndAt,
			RenewDuration: uint32(durations),
		})
	}

	return nil
}

func (h *OrderHandler) CalculateDeduction() (bool, error) {
	feeUSDAmount := h.ElectricityFeeUSDAmount.Add(h.TechniqueFeeUSDAmount)
	if feeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return false, nil
	}
	for _, coin := range h.DeductionCoins {
		ledger, ok := h.UserLedgers[coin.CoinTypeID]
		if !ok {
			continue
		}
		currency, ok := h.Currencies[coin.CoinTypeID]
		if !ok {
			return true, fmt.Errorf("invalid coincurrency")
		}
		currencyValue, err := decimal.NewFromString(currency.MarketValueLow)
		if err != nil {
			return true, err
		}
		if currencyValue.Cmp(decimal.NewFromInt(0)) <= 0 {
			return true, fmt.Errorf("invalid coinusdcurrency")
		}
		spendable, err := decimal.NewFromString(ledger.Spendable)
		if err != nil {
			return true, err
		}
		if spendable.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}
		feeCoinAmount := feeUSDAmount.Div(currencyValue)
		appCoin, ok := h.DeductionAppCoins[ledger.CoinTypeID]
		if !ok {
			return true, fmt.Errorf("invalid deductioncoin")
		}
		if spendable.Cmp(feeCoinAmount) >= 0 {
			h.Deductions = append(h.Deductions, &orderrenewpb.Deduction{
				AppCoin:     appCoin,
				USDCurrency: currency.MarketValueLow,
				Amount:      feeCoinAmount.String(),
			})
			return false, nil
		}
		h.Deductions = append(h.Deductions, &orderrenewpb.Deduction{
			AppCoin:     appCoin,
			USDCurrency: currency.MarketValueLow,
			Amount:      spendable.String(),
		})
		spendableUSD := spendable.Mul(currencyValue)
		feeUSDAmount = feeUSDAmount.Sub(spendableUSD)
	}
	h.InsufficientBalance = true
	return true, nil
}

//nolint:gocognit
func (h *OrderHandler) CalculateDeductionForOrder() (bool, error) {
	electricityFeeUSDAmount := h.ElectricityFeeUSDAmount
	techniqueFeeUSDAmount := h.TechniqueFeeUSDAmount

	if electricityFeeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 &&
		techniqueFeeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return false, nil
	}

	for _, coin := range h.DeductionCoins {
		ledger, ok := h.UserLedgers[coin.CoinTypeID]
		if !ok {
			continue
		}
		currency, ok := h.Currencies[coin.CoinTypeID]
		if !ok {
			return true, fmt.Errorf("invalid coincurrency")
		}
		currencyValue, err := decimal.NewFromString(currency.MarketValueLow)
		if err != nil {
			return true, err
		}
		if currencyValue.Cmp(decimal.NewFromInt(0)) <= 0 {
			return true, fmt.Errorf("invalid coinusdcurrency")
		}
		spendable, err := decimal.NewFromString(ledger.Spendable)
		if err != nil {
			return true, err
		}
		if spendable.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}

		electricityFeeCoinAmount := electricityFeeUSDAmount.Div(currencyValue)
		techniqueFeeCoinAmount := techniqueFeeUSDAmount.Div(currencyValue)
		spendableUSD := spendable.Mul(currencyValue)

		appCoin, ok := h.DeductionAppCoins[ledger.CoinTypeID]
		if !ok {
			return true, fmt.Errorf("invalid deductioncoin")
		}
		if spendable.Cmp(electricityFeeCoinAmount) >= 0 &&
			electricityFeeCoinAmount.Cmp(decimal.NewFromInt(0)) > 0 {
			h.Deductions = append(h.Deductions, &orderrenewpb.Deduction{
				AppGood:     h.ElectricityFeeAppGood,
				AppCoin:     appCoin,
				USDCurrency: currency.MarketValueLow,
				Amount:      electricityFeeCoinAmount.String(),
			})
			spendable = spendable.Sub(electricityFeeCoinAmount)
			spendableUSD = spendableUSD.Sub(electricityFeeUSDAmount)
			electricityFeeUSDAmount = decimal.NewFromInt(0)
		}
		if spendable.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}
		// Only when all electricity fee is created, then we create technique fee
		if spendable.Cmp(techniqueFeeCoinAmount) >= 0 &&
			techniqueFeeCoinAmount.Cmp(decimal.NewFromInt(0)) > 0 &&
			electricityFeeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
			h.Deductions = append(h.Deductions, &orderrenewpb.Deduction{
				AppGood:     h.TechniqueFeeAppGood,
				AppCoin:     appCoin,
				USDCurrency: currency.MarketValueLow,
				Amount:      techniqueFeeCoinAmount.String(),
			})
			spendable = spendable.Sub(techniqueFeeCoinAmount)
			spendableUSD = spendableUSD.Sub(techniqueFeeUSDAmount)
			techniqueFeeUSDAmount = decimal.NewFromInt(0)
		}
		if electricityFeeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 &&
			techniqueFeeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
			return false, nil
		}
		if spendable.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}

		if electricityFeeUSDAmount.Cmp(decimal.NewFromInt(0)) > 0 {
			h.Deductions = append(h.Deductions, &orderrenewpb.Deduction{
				AppGood:     h.ElectricityFeeAppGood,
				AppCoin:     appCoin,
				USDCurrency: currency.MarketValueLow,
				Amount:      spendable.String(),
			})
			electricityFeeUSDAmount = electricityFeeUSDAmount.Sub(spendableUSD)
			continue
		}
		h.Deductions = append(h.Deductions, &orderrenewpb.Deduction{
			AppGood:     h.TechniqueFeeAppGood,
			AppCoin:     appCoin,
			USDCurrency: currency.MarketValueLow,
			Amount:      spendable.String(),
		})
		techniqueFeeUSDAmount = techniqueFeeUSDAmount.Sub(spendableUSD)
	}
	h.InsufficientBalance = true
	return true, nil
}
