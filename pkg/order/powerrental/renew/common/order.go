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
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	appfeemwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/fee"
	appgoodrequiredmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good/required"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	chaintypes "github.com/NpoolPlatform/message/npool/basetypes/chain/v1"
	goodtypes "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appcoinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/app/coin"
	currencymwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency"
	coinusedformwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/usedfor"
	appfeemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/fee"
	appgoodrequiredmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good/required"
	apppowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	ledgermwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	orderrenewpb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/order/renew"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	feeordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/fee"

	"github.com/shopspring/decimal"
)

type OrderHandler struct {
	*powerrentalordermwpb.PowerRentalOrder

	AppPowerRental   *apppowerrentalmwpb.PowerRental
	appGoodRequireds []*appgoodrequiredmwpb.Required

	TechniqueFee                *appfeemwpb.Fee
	ElectricityFee              *appfeemwpb.Fee
	TechniqueFeeSeconds         uint32
	ElectricityFeeSeconds       uint32
	TechniqueFeeEndAt           uint32
	ElectricityFeeEndAt         uint32
	TechniqueFeeExtendSeconds   uint32
	ElectricityFeeExtendSeconds uint32

	CheckElectricityFee bool
	CheckTechniqueFee   bool

	DeductionCoins          []*coinusedformwpb.CoinUsedFor
	DeductionAppCoins       map[string]*appcoinmwpb.Coin
	Deductions              []*orderrenewpb.Deduction
	UserLedgers             map[string]*ledgermwpb.Ledger
	Currencies              map[string]*currencymwpb.Currency
	ElectricityFeeUSDAmount decimal.Decimal
	TechniqueFeeUSDAmount   decimal.Decimal
	InsufficientBalance     bool
	RenewInfos              []*orderrenewpb.RenewInfo
}

func (h *OrderHandler) GetAppPowerRental(ctx context.Context) (err error) {
	h.AppPowerRental, err = apppowerrentalmwcli.GetPowerRental(ctx, h.AppGoodID)
	if err != nil {
		return wlog.WrapError(err)
	}
	if h.AppPowerRental == nil {
		return wlog.Errorf("invalid apppowerrental")
	}
	return nil
}

func (h *OrderHandler) GetAppGoodRequireds(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		requireds, _, err := appgoodrequiredmwcli.GetRequireds(ctx, &appgoodrequiredmwpb.Conds{
			MainAppGoodID: &basetypes.StringVal{Op: cruder.EQ, Value: h.AppGoodID},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(requireds) == 0 {
			break
		}
		h.appGoodRequireds = append(h.appGoodRequireds, requireds...)
		offset += limit
	}
	return nil
}

func (h *OrderHandler) GetAppFees(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	appFees, _, err := appfeemwcli.GetFees(ctx, &appfeemwpb.Conds{
		AppID: &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		AppGoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: func() (appGoodIDs []string) {
			for _, appGoodRequired := range h.appGoodRequireds {
				appGoodIDs = append(appGoodIDs, appGoodRequired.RequiredAppGoodID)
			}
			return
		}()},
	}, offset, limit)
	if err != nil {
		return err
	}
	for _, appFee := range appFees {
		switch appFee.GoodType {
		case goodtypes.GoodType_ElectricityFee:
			h.ElectricityFee = appFee
		case goodtypes.GoodType_TechniqueServiceFee:
			h.TechniqueFee = appFee
		}
	}
	return nil
}

func (h *OrderHandler) Renewable(ctx context.Context) (bool, error) {
	if h.AppPowerRental.PackageWithRequireds {
		return false, nil
	}
	if exist, err := feeordermwcli.ExistFeeOrderConds(ctx, &feeordermwpb.Conds{
		ParentOrderID: &basetypes.StringVal{Op: cruder.EQ, Value: h.OrderID},
		PaymentState:  &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ordertypes.PaymentState_PaymentStateWait)},
	}); err != nil || exist {
		return false, wlog.WrapError(err)
	}
	return h.TechniqueFee != nil || h.ElectricityFee != nil, nil
}

func (h *OrderHandler) FormalizeFeeDurationSeconds() {
	for _, feeDuration := range h.FeeDurations {
		if h.ElectricityFee != nil && h.ElectricityFee.AppGoodID == feeDuration.AppGoodID {
			h.ElectricityFeeSeconds = feeDuration.TotalDurationSeconds
		}
		if h.TechniqueFee != nil && h.TechniqueFee.AppGoodID == feeDuration.AppGoodID {
			h.TechniqueFeeSeconds = feeDuration.TotalDurationSeconds
		}
	}
}

//nolint:gocognit,gocyclo
func (h *OrderHandler) CalculateRenewDuration(ctx context.Context) error {
	ignoredSeconds := h.OutOfGasSeconds + h.CompensateSeconds
	h.ElectricityFeeEndAt = h.StartAt + h.ElectricityFeeSeconds + ignoredSeconds
	h.TechniqueFeeEndAt = h.StartAt + h.TechniqueFeeSeconds + ignoredSeconds

	now := uint32(time.Now().Unix())
	const secondsBeforeFeeExhausted = timedef.SecondsPerHour * 24

	if h.ElectricityFee != nil && h.ElectricityFee.SettlementType == goodtypes.GoodSettlementType_GoodSettledByProfitPercent {
		h.ElectricityFeeEndAt = h.EndAt
	}
	if h.TechniqueFee != nil && h.TechniqueFee.SettlementType == goodtypes.GoodSettlementType_GoodSettledByProfitPercent {
		h.TechniqueFeeEndAt = h.EndAt
	}

	if h.ElectricityFee != nil && h.ElectricityFeeEndAt < h.EndAt {
		h.CheckElectricityFee =
			h.ElectricityFee.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfitPercent &&
				h.ElectricityFeeEndAt < now+secondsBeforeFeeExhausted
	}
	if h.TechniqueFee != nil && h.TechniqueFeeEndAt < h.EndAt {
		h.CheckTechniqueFee =
			h.TechniqueFee.SettlementType != goodtypes.GoodSettlementType_GoodSettledByProfitPercent &&
				h.TechniqueFeeEndAt < now+secondsBeforeFeeExhausted
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

func goodDurationDisplay2Duration(_type goodtypes.GoodDurationType, seconds uint32) (units uint32) {
	switch _type {
	case goodtypes.GoodDurationType_GoodDurationByHour:
		units = seconds / timedef.SecondsPerHour
	case goodtypes.GoodDurationType_GoodDurationByDay:
		units = seconds / timedef.SecondsPerDay
	case goodtypes.GoodDurationType_GoodDurationByMonth:
		units = seconds / timedef.SecondsPerMonth
	case goodtypes.GoodDurationType_GoodDurationByYear:
		units = seconds / timedef.SecondsPerYear
	}
	return units
}

//nolint:gocognit
func (h *OrderHandler) CalculateUSDAmount() error {
	orderUnits, err := decimal.NewFromString(h.Units)
	if err != nil {
		return err
	}

	now := uint32(time.Now().Unix())
	remainSeconds := uint32(h.EndAt - now)
	durationSeconds := uint32(3 * timedef.SecondsPerDay) //nolint
	if durationSeconds > remainSeconds {
		durationSeconds = remainSeconds
	}

	//nolint:dupl
	if h.CheckElectricityFee {
		unitPrice, err := decimal.NewFromString(h.ElectricityFee.UnitValue)
		if err != nil {
			return err
		}
		durations := goodDurationDisplay2Duration(h.ElectricityFee.DurationDisplayType, durationSeconds)
		h.ElectricityFeeExtendSeconds = durationSeconds
		h.ElectricityFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
		h.RenewInfos = append(h.RenewInfos, &orderrenewpb.RenewInfo{
			AppGoodInfo: &orderrenewpb.AppGoodInfo{
				AppGoodID: h.ElectricityFee.AppGoodID,
				GoodType:  h.ElectricityFee.GoodType,
			},
			EndAt:         h.ElectricityFeeEndAt,
			RenewDuration: uint32(durations),
		})
	}

	//nolint:dupl
	if h.CheckTechniqueFee {
		unitPrice, err := decimal.NewFromString(h.TechniqueFee.UnitValue)
		if err != nil {
			return err
		}
		durations := goodDurationDisplay2Duration(h.TechniqueFee.DurationDisplayType, durationSeconds)
		h.TechniqueFeeExtendSeconds = durationSeconds
		h.TechniqueFeeUSDAmount = unitPrice.Mul(decimal.NewFromInt(int64(durations))).Mul(orderUnits)
		h.RenewInfos = append(h.RenewInfos, &orderrenewpb.RenewInfo{
			AppGoodInfo: &orderrenewpb.AppGoodInfo{
				AppGoodID: h.TechniqueFee.AppGoodID,
				GoodType:  h.TechniqueFee.GoodType,
			},
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
				AppGoodInfo: &orderrenewpb.AppGoodInfo{
					AppGoodID: h.ElectricityFee.AppGoodID,
					GoodType:  h.ElectricityFee.GoodType,
				},
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
				AppGoodInfo: &orderrenewpb.AppGoodInfo{
					AppGoodID: h.TechniqueFee.AppGoodID,
					GoodType:  h.TechniqueFee.GoodType,
				},
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
				AppGoodInfo: &orderrenewpb.AppGoodInfo{
					AppGoodID: h.ElectricityFee.AppGoodID,
					GoodType:  h.ElectricityFee.GoodType,
				},
				AppCoin:     appCoin,
				USDCurrency: currency.MarketValueLow,
				Amount:      spendable.String(),
			})
			electricityFeeUSDAmount = electricityFeeUSDAmount.Sub(spendableUSD)
			continue
		}
		h.Deductions = append(h.Deductions, &orderrenewpb.Deduction{
			AppGoodInfo: &orderrenewpb.AppGoodInfo{
				AppGoodID: h.TechniqueFee.AppGoodID,
				GoodType:  h.TechniqueFee.GoodType,
			},
			AppCoin:     appCoin,
			USDCurrency: currency.MarketValueLow,
			Amount:      spendable.String(),
		})
		techniqueFeeUSDAmount = techniqueFeeUSDAmount.Sub(spendableUSD)
	}
	h.InsufficientBalance = true
	return true, nil
}
