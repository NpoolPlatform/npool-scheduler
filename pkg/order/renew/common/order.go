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
	UserLedgers                    []*ledgermwpb.Ledger
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

//nolint:gocognit
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

	if h.ElectricityFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.ElectricityFeeAppGood.EntID && order.PaymentState == ordertypes.PaymentState_PaymentStateDone {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.ElectricityFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
			if order.PaymentState == ordertypes.PaymentState_PaymentStateWait {
				h.ExistUnpaidElectricityFeeOrder = true
			}
		}
	}

	if h.TechniqueFeeAppGood != nil {
		lastEndAt := uint32(0)
		for _, order := range h.childOrders {
			if order.AppGoodID == h.TechniqueFeeAppGood.EntID && order.PaymentState == ordertypes.PaymentState_PaymentStateDone {
				if order.StartAt < lastEndAt {
					return fmt.Errorf("invalid order duration")
				}
				h.TechniqueFeeDuration += order.EndAt - order.StartAt
				lastEndAt = order.EndAt
			}
			if order.PaymentState == ordertypes.PaymentState_PaymentStateWait {
				h.ExistUnpaidTechniqueFeeOrder = true
			}
		}
	}

	outOfGas := h.OutOfGasHours * timedef.SecondsPerHour
	compensate := h.CompensateHours * timedef.SecondsPerHour
	ignoredSeconds := outOfGas + compensate

	h.ElectricityFeeEndAt = h.StartAt + h.ElectricityFeeDuration + ignoredSeconds
	h.TechniqueFeeEndAt = h.StartAt + h.TechniqueFeeDuration + ignoredSeconds

	now := uint32(time.Now().Unix())
	const secondsBeforeFeeExhausted = timedef.SecondsPerHour * 24

	if h.ElectricityFeeAppGood != nil && h.ElectricityFeeEndAt < h.EndAt {
		h.CheckElectricityFee = h.StartAt+h.ElectricityFeeDuration+ignoredSeconds < now+secondsBeforeFeeExhausted
	}
	if h.TechniqueFeeAppGood != nil && h.TechniqueFeeEndAt < h.EndAt {
		h.CheckTechniqueFee = h.StartAt+h.TechniqueFeeDuration+ignoredSeconds < now+secondsBeforeFeeExhausted
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

func (h *OrderHandler) CalculateUSDAmount() error {
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
		seconds := 0
		switch h.ElectricityFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= timedef.HoursPerDay * 3
			seconds = durations * timedef.SecondsPerHour
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
			seconds = durations * timedef.SecondsPerDay
		case goodtypes.GoodDurationType_GoodDurationByMonth:
			seconds = durations * timedef.SecondsPerMonth
		case goodtypes.GoodDurationType_GoodDurationByYear:
			seconds = durations * timedef.SecondsPerYear
		}
		h.ElectricityFeeExtendDuration = uint32(durations)
		h.ElectricityFeeExtendSeconds = uint32(seconds)
		h.ElectricityFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
		h.RenewInfos = append(h.RenewInfos, &orderrenewpb.RenewInfo{
			AppGood: h.ElectricityFeeAppGood,
			EndAt:   h.ElectricityFeeEndAt,
		})
	}

	if h.CheckTechniqueFee {
		unitPrice, err := decimal.NewFromString(h.TechniqueFeeAppGood.UnitPrice)
		if err != nil {
			return err
		}
		durations := 1 //nolint
		seconds := 0
		switch h.TechniqueFeeAppGood.DurationType {
		case goodtypes.GoodDurationType_GoodDurationByHour:
			durations *= timedef.HoursPerDay * 3
			seconds = durations * timedef.SecondsPerHour
		case goodtypes.GoodDurationType_GoodDurationByDay:
			durations = 3
			seconds = durations * timedef.SecondsPerDay
		case goodtypes.GoodDurationType_GoodDurationByMonth:
			seconds = durations * timedef.SecondsPerMonth
		case goodtypes.GoodDurationType_GoodDurationByYear:
			seconds = durations * timedef.SecondsPerYear
		}
		h.TechniqueFeeExtendDuration = uint32(durations)
		h.TechniqueFeeExtendSeconds = uint32(seconds)
		h.TechniqueFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
		h.RenewInfos = append(h.RenewInfos, &orderrenewpb.RenewInfo{
			AppGood: h.TechniqueFeeAppGood,
			EndAt:   h.TechniqueFeeEndAt,
		})
	}

	return nil
}

func (h *OrderHandler) CalculateDeduction() (bool, error) {
	feeUSDAmount := h.ElectricityFeeUSDAmount.Add(h.TechniqueFeeUSDAmount)
	if feeUSDAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return false, nil
	}
	for _, ledger := range h.UserLedgers {
		currency, ok := h.Currencies[ledger.CoinTypeID]
		if !ok {
			return true, fmt.Errorf("invalid coinusdcurrency")
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

	for _, ledger := range h.UserLedgers {
		currency, ok := h.Currencies[ledger.CoinTypeID]
		if !ok {
			return true, fmt.Errorf("invalid coinusdcurrency")
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
