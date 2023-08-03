//nolint:errcheck
package order

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	calculatemwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/calculate"
	calculatemwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/calculate"
	"github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	"github.com/NpoolPlatform/message/npool/notif/mw/v1/template"

	goodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/appgood"
	goodmgrpb "github.com/NpoolPlatform/message/npool/good/mgr/v1/appgood"
	paymentmgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/payment"

	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"

	commonpb "github.com/NpoolPlatform/message/npool"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	accountlock "github.com/NpoolPlatform/account-middleware/pkg/lock"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerv2mwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger/v2"
	ledgermwpkg "github.com/NpoolPlatform/ledger-middleware/pkg/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"

	usermwcli "github.com/NpoolPlatform/appuser-middleware/pkg/client/user"
	usermwpb "github.com/NpoolPlatform/message/npool/appuser/mw/v1/user"

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"

	"github.com/shopspring/decimal"
)

const TimeoutSeconds = 6 * 60 * 60

func processState(order *ordermwpb.Order, balance decimal.Decimal) (paymentmgrpb.PaymentState, ordermgrpb.OrderState) {
	if order.UserCanceled {
		return paymentmgrpb.PaymentState_Canceled, ordermgrpb.OrderState_Canceled
	}
	amount, _ := decimal.NewFromString(order.PaymentAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	if amount.Add(startAmount).Cmp(balance) <= 0 {
		return paymentmgrpb.PaymentState_Done, ordermgrpb.OrderState_Paid
	}
	if order.CreatedAt+TimeoutSeconds < uint32(time.Now().Unix()) {
		return paymentmgrpb.PaymentState_TimeOut, ordermgrpb.OrderState_PaymentTimeout
	}
	return order.PaymentState, order.OrderState
}

func processFinishAmount(order *ordermwpb.Order, balance decimal.Decimal) decimal.Decimal {
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	amount, _ := decimal.NewFromString(order.PaymentAmount)
	if order.UserCanceled {
		return balance.Sub(startAmount)
	}
	if amount.Add(startAmount).Cmp(balance) <= 0 {
		return balance.Sub(startAmount).Sub(amount)
	}
	if order.CreatedAt+TimeoutSeconds < uint32(time.Now().Unix()) {
		return balance.Sub(startAmount)
	}
	return decimal.NewFromInt(0)
}

func processStock(order *ordermwpb.Order, balance decimal.Decimal) (unlocked, waitstart decimal.Decimal, err error) {
	units, err := decimal.NewFromString(order.Units)
	if err != nil {
		return decimal.Decimal{}, decimal.Decimal{}, err
	}
	if order.UserCanceled {
		return units, decimal.NewFromInt(0), nil
	}
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	amount, _ := decimal.NewFromString(order.PaymentAmount)
	if amount.Add(startAmount).Cmp(decimal.NewFromFloat(balance.InexactFloat64()+10e-7)) <= 0 {
		return units, units, nil
	}
	if order.CreatedAt+TimeoutSeconds < uint32(time.Now().Unix()) {
		return units, decimal.NewFromInt(0), nil
	}
	return decimal.NewFromInt(0), decimal.NewFromInt(0), nil
}

func tryFinishPayment(
	ctx context.Context,
	order *ordermwpb.Order,
	newState paymentmgrpb.PaymentState,
	newOrderState ordermgrpb.OrderState,
	fakePayment bool,
	finishAmount decimal.Decimal,
) error {
	if newState != order.PaymentState {
		finishAmountS := finishAmount.String()

		logger.Sugar().Infow(
			"tryFinishPayment",
			"payment", order.ID,
			"paymentState", order.PaymentState,
			"newState", newState,
			"paymentFinishAmount", finishAmount,
		)
		_, err := ordermwcli.UpdateOrder(ctx, &ordermwpb.OrderReq{
			ID:                  &order.ID,
			PaymentState:        &newState,
			FakePayment:         &fakePayment,
			State:               &newOrderState,
			PaymentID:           &order.PaymentID,
			PaymentFinishAmount: &finishAmountS,
		})
		if err != nil {
			return err
		}

		order.PaymentState = newState
		order.OrderState = newOrderState
		order.PaymentFinishAmount = finishAmountS

		if order.OrderState == ordermgrpb.OrderState_Paid {
			orderPaidNotif(ctx, order)
		}
	}

	switch newState {
	case paymentmgrpb.PaymentState_Done:
	case paymentmgrpb.PaymentState_Canceled:
	case paymentmgrpb.PaymentState_TimeOut:
	default:
		return nil
	}

	account, err := payaccmwcli.GetAccountOnly(ctx, &payaccmwpb.Conds{
		AccountID: &basetypes.StringVal{Op: cruder.EQ, Value: order.PaymentAccountID},
		Active:    &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:    &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:   &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if account == nil {
		logger.Sugar().Errorw("tryFinishPayment", "AccountID", order.PaymentAccountID, "error", err)
		return fmt.Errorf("invalid account")
	}

	locked := false

	_, err = payaccmwcli.UpdateAccount(ctx, &payaccmwpb.AccountReq{
		ID:     &account.ID,
		Locked: &locked,
	})
	return err
}

func tryUpdatePaymentLedger(ctx context.Context, order *ordermwpb.Order) error {
	finishAmount, _ := decimal.NewFromString(order.PaymentFinishAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	logger.Sugar().Infow(
		"tryUpdatePaymentLedger",
		"OrderID", order.ID,
		"startAmount", startAmount,
		"finishAmount", finishAmount,
		"PaymentState", order.PaymentState,
	)
	if finishAmount.Cmp(startAmount) <= 0 {
		return nil
	}

	switch order.PaymentState {
	case paymentmgrpb.PaymentState_Done:
	case paymentmgrpb.PaymentState_Canceled:
	case paymentmgrpb.PaymentState_TimeOut:
	default:
		return nil
	}

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v","OrderID": "%v","PaymentState":"%v","GoodID":"%v"}`,
		order.ID, order.ID, order.PaymentState, order.GoodID)
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Payment

	amount := finishAmount.
		Sub(startAmount).
		String()

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
}

func tryUpdateOrderLedger(ctx context.Context, order *ordermwpb.Order) error {
	amount, _ := decimal.NewFromString(order.PaymentAmount)

	if amount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	switch order.PaymentState {
	case paymentmgrpb.PaymentState_Done:
	default:
		return nil
	}

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, order.ID, order.ID)
	amountStr := amount.String()
	ioType := ledgerdetailpb.IOType_Outcoming
	ioSubType := ledgerdetailpb.IOSubType_Payment

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amountStr,
		IOExtra:    &ioExtra,
	})
}

func unlockBalance(ctx context.Context, order *ordermwpb.Order, paymentState paymentmgrpb.PaymentState) error {
	if order.PayWithBalanceAmount == "" {
		return nil
	}

	balance, err := decimal.NewFromString(order.PayWithBalanceAmount)
	if err != nil {
		return err
	}

	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	outcoming := decimal.NewFromInt(0)
	unlocked, err := decimal.NewFromString(order.PayWithBalanceAmount)
	if err != nil {
		return err
	}

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v", "BalanceAmount": "%v"}`,
		order.PaymentID, order.ID, order.PayWithBalanceAmount)

	switch paymentState {
	case paymentmgrpb.PaymentState_Canceled:
	case paymentmgrpb.PaymentState_TimeOut:
	case paymentmgrpb.PaymentState_Done:
		outcoming = unlocked
	default:
		return nil
	}

	return ledgermwcli.UnlockBalance(
		ctx,
		order.AppID, order.UserID, order.PaymentCoinTypeID,
		ledgerdetailpb.IOSubType_Payment,
		unlocked, outcoming,
		ioExtra,
	)
}

// nolint
func _processOrderPayment(ctx context.Context, order *ordermwpb.Order) error {
	good, err := goodmwcli.GetGoodOnly(ctx, &goodmgrpb.Conds{
		AppID:  &commonpb.StringVal{Op: cruder.EQ, Value: order.AppID},
		GoodID: &commonpb.StringVal{Op: cruder.EQ, Value: order.GoodID},
	})
	if err != nil {
		return err
	}
	if good == nil {
		return fmt.Errorf("invalid good")
	}

	coin, err := coinmwcli.GetCoin(ctx, order.PaymentCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	if !coin.ForPay {
		return fmt.Errorf("invalid payment coin")
	}

	account, err := payaccmwcli.GetAccountOnly(ctx, &payaccmwpb.Conds{
		AccountID: &basetypes.StringVal{Op: cruder.EQ, Value: order.PaymentAccountID},
		Active:    &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Locked:    &basetypes.BoolVal{Op: cruder.EQ, Value: true},
		Blocked:   &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if account == nil {
		logger.Sugar().Errorw("_processOrderPayment", "AccountID", order.PaymentAccountID, "error", err)
		return fmt.Errorf("invalid account")
	}

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: account.Address,
	})
	if err != nil {
		return err
	}
	if balance == nil {
		return fmt.Errorf("invalid balance")
	}

	bal, err := decimal.NewFromString(balance.BalanceStr)
	if err != nil {
		return err
	}

	state, newOrderState := processState(order, bal)
	remain := processFinishAmount(order, bal)
	finishAmount := bal
	unlocked, waitstart, err := processStock(order, bal)
	if err != nil {
		return err
	}
	amount, _ := decimal.NewFromString(order.PaymentAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	dueAmount := amount.Add(startAmount).String()
	logger.Sugar().Infow("processOrderPayment", "order", order.ID, "payment",
		order.ID, "coin", coin.Name, "startAmount", order.PaymentStartAmount,
		"finishAmount", order.PaymentFinishAmount, "amount", order.PaymentAmount,
		"dueAmount", dueAmount, "paymentState", order.PaymentState,
		"newState", state, "newOrderState", newOrderState, "remain", remain, "unlocked", unlocked,
		"waitstart", waitstart, "balance", bal,
		"coin", coin.Name, "address", account.Address, "balance", balance,
	)

	if state == paymentmgrpb.PaymentState_Done {
		_, err = ledgermwpkg.TryCreateProfit(ctx, order.AppID, order.UserID, good.CoinTypeID)
		if err != nil {
			return err
		}
	}

	if err := tryFinishPayment(ctx, order, state, newOrderState, false, finishAmount); err != nil {
		return err
	}

	// TODO: move to TX begin

	if err := tryUpdatePaymentLedger(ctx, order); err != nil {
		return err
	}

	if err := tryUpdateOrderLedger(ctx, order); err != nil {
		return err
	}

	if err := unlockBalance(ctx, order, state); err != nil {
		return err
	}

	switch state {
	case paymentmgrpb.PaymentState_Done:
		fallthrough //nolint
	case paymentmgrpb.PaymentState_Canceled:
		fallthrough //nolint
	case paymentmgrpb.PaymentState_TimeOut:
		if err := updateStock(ctx, order.GoodID, unlocked, decimal.NewFromInt(0), waitstart); err != nil {
			logger.Sugar().Errorw(
				"processOrderPayment",
				"Step", "Update Stock",
				"OrderID", order.ID,
				"Error", err)
			return err
		}
	}

	if state != paymentmgrpb.PaymentState_Done {
		return nil
	}

	paymentAmount := decimal.RequireFromString(order.PaymentAmount)
	paymentAmount = paymentAmount.Add(decimal.RequireFromString(order.PayWithBalanceAmount))
	paymentAmountS := paymentAmount.String()

	goodValue := decimal.RequireFromString(good.Price).
		Mul(decimal.RequireFromString(order.Units)).
		String()

	comms, err := calculatemwcli.Calculate(ctx, &calculatemwpb.CalculateRequest{
		AppID:                  order.AppID,
		UserID:                 order.UserID,
		GoodID:                 order.GoodID,
		OrderID:                order.ID,
		PaymentID:              order.PaymentID,
		CoinTypeID:             good.CoinTypeID,
		PaymentCoinTypeID:      order.PaymentCoinTypeID,
		PaymentCoinUSDCurrency: order.PaymentCoinUSDCurrency,
		Units:                  order.Units,
		PaymentAmount:          paymentAmountS,
		GoodValue:              goodValue,
		SettleType:             good.CommissionSettleType,
		HasCommission:          true,
		OrderCreatedAt:         order.CreatedAt,
	})
	if err != nil {
		return err
	}

	details := []*ledgerdetailpb.DetailReq{}
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Commission

	for _, comm := range comms {
		commAmount, err := decimal.NewFromString(comm.Amount)
		if err != nil {
			return err
		}
		if commAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}
		ioExtra := fmt.Sprintf(
			`{"PaymentID":"%v","OrderID":"%v","DirectContributorID":"%v","OrderUserID":"%v"}`,
			order.PaymentID,
			order.ID,
			comm.GetDirectContributorID(),
			order.UserID,
		)

		details = append(details, &ledgerdetailpb.DetailReq{
			AppID:      &order.AppID,
			UserID:     &comm.UserID,
			CoinTypeID: &order.PaymentCoinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &comm.Amount,
			IOExtra:    &ioExtra,
		})
	}

	if len(details) > 0 {
		err = ledgerv2mwcli.BookKeeping(ctx, details)
		if err != nil {
			logger.Sugar().Errorw(
				"processOrderPayment",
				"Step", "BookKeeping Commissions",
				"OrderID", order.ID,
				"Error", err)
			return err
		}
	}

	count, err := ordermwcli.CountOrders(ctx, &ordermwpb.Conds{
		AppID:  &commonpb.StringVal{Op: cruder.EQ, Value: order.AppID},
		UserID: &commonpb.StringVal{Op: cruder.EQ, Value: order.UserID},
		GoodID: &commonpb.StringVal{Op: cruder.EQ, Value: order.GoodID},
		States: &commonpb.Uint32SliceVal{
			Op: cruder.IN,
			Value: []uint32{
				uint32(ordermgrpb.OrderState_Paid),
				uint32(ordermgrpb.OrderState_InService),
				uint32(ordermgrpb.OrderState_Expired),
			},
		},
	})
	if err != nil {
		logger.Sugar().Errorw(
			"processOrderPayment",
			"Step", "Count Consecutive Orders",
			"OrderID", order.ID,
			"Error", err)
		return nil
	}

	amount1 := decimal.RequireFromString(order.PaymentAmount)
	amount1 = amount1.Add(decimal.RequireFromString(order.PayWithBalanceAmount))
	_amount := amount1.String()

	credits, err := eventmwcli.RewardEvent(ctx, &eventmwpb.RewardEventRequest{
		AppID:       order.AppID,
		UserID:      order.UserID,
		EventType:   basetypes.UsedFor_Purchase,
		GoodID:      &order.GoodID,
		Consecutive: count,
		Amount:      _amount,
	})
	if err != nil {
		logger.Sugar().Errorw(
			"processOrderPayment",
			"Step", "Reward Event",
			"OrderID", order.ID,
			"Consecutive", count,
			"Amount", order.PaymentAmount,
			"EventType", basetypes.UsedFor_Purchase,
			"Error", err)
	}

	_credits, err := eventmwcli.RewardEvent(ctx, &eventmwpb.RewardEventRequest{
		AppID:       order.AppID,
		UserID:      order.UserID,
		EventType:   basetypes.UsedFor_AffiliatePurchase,
		GoodID:      &order.GoodID,
		Consecutive: count,
		Amount:      _amount,
	})
	if err != nil {
		logger.Sugar().Errorw(
			"processOrderPayment",
			"Step", "Reward Event",
			"OrderID", order.ID,
			"Consecutive", count,
			"Amount", order.PaymentAmount,
			"EventType", basetypes.UsedFor_AffiliatePurchase,
			"Error", err)
	}

	credits = append(credits, _credits...)
	if len(credits) == 0 {
		return nil
	}

	for _, credit := range credits {
		_, err = usermwcli.UpdateUser(ctx, &usermwpb.UserReq{
			ID:            &credit.UserID,
			AppID:         &credit.AppID,
			ActionCredits: &credit.Credits,
		})
		if err != nil {
			logger.Sugar().Errorw(
				"processOrderPayment",
				"Step", "Credits Increment",
				"AppID", credit.AppID,
				"UserID", credit.UserID,
				"OrderID", order.ID,
				"Credits", credit.Credits,
				"Error", err)
			return nil
		}
	}

	// TODO: move to TX end

	return nil
}

func _processFakeOrder(ctx context.Context, order *ordermwpb.Order) error {
	coin, err := coinmwcli.GetCoin(ctx, order.PaymentCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}

	state := paymentmgrpb.PaymentState_Done
	orderState := ordermgrpb.OrderState_Paid
	units, err := decimal.NewFromString(order.Units)
	if err != nil {
		return err
	}
	unlocked, waitstart := units, units

	amount, _ := decimal.NewFromString(order.PaymentAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	dueAmount := amount.Add(startAmount).String()
	logger.Sugar().Infow("processFakeOrder", "order", order.ID, "payment",
		order.PaymentID, "coin", coin.Name, "startAmount", order.PaymentStartAmount,
		"finishAmount", order.PaymentFinishAmount, "amount", order.PaymentAmount,
		"dueAmount", dueAmount, "state", order.PaymentState,
		"newState", state, "newOrderState", orderState,
		"unlocked", unlocked, "waitstart", waitstart)

	finishAmount, _ := decimal.NewFromString(order.PaymentStartAmount)

	if err := tryFinishPayment(ctx, order, state, orderState, true, finishAmount); err != nil {
		return err
	}

	err = updateStock(ctx, order.GoodID, unlocked, decimal.NewFromInt(0), waitstart)
	if err != nil {
		return err
	}

	if order.OrderType != ordermgrpb.OrderType_Offline {
		return nil
	}

	good, err := goodmwcli.GetGoodOnly(ctx, &goodmgrpb.Conds{
		AppID:  &commonpb.StringVal{Op: cruder.EQ, Value: order.AppID},
		GoodID: &commonpb.StringVal{Op: cruder.EQ, Value: order.GoodID},
	})
	if err != nil {
		return err
	}
	if good == nil {
		return fmt.Errorf("invalid good")
	}

	paymentAmount := decimal.RequireFromString(order.PaymentAmount)
	paymentAmount = paymentAmount.Add(decimal.RequireFromString(order.PayWithBalanceAmount))
	paymentAmountS := paymentAmount.String()

	goodValue := decimal.RequireFromString(good.Price).
		Mul(units).
		String()

	_, err = calculatemwcli.Calculate(ctx, &calculatemwpb.CalculateRequest{
		AppID:                  order.AppID,
		UserID:                 order.UserID,
		GoodID:                 order.GoodID,
		OrderID:                order.ID,
		PaymentID:              order.PaymentID,
		CoinTypeID:             good.CoinTypeID,
		PaymentCoinTypeID:      order.PaymentCoinTypeID,
		PaymentCoinUSDCurrency: order.PaymentCoinUSDCurrency,
		Units:                  order.Units,
		PaymentAmount:          paymentAmountS,
		GoodValue:              goodValue,
		SettleType:             good.CommissionSettleType,
		HasCommission:          false,
		OrderCreatedAt:         order.CreatedAt,
	})
	if err != nil {
		logger.Sugar().Infow("_processFakeOrder", "Error", err)
	}

	return nil
}

func processOrderPayment(ctx context.Context, order *ordermwpb.Order) error {
	err := accountlock.Lock(order.PaymentAccountID)
	if err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(order.PaymentAccountID)
	}()

	switch order.OrderType {
	case ordermgrpb.OrderType_Normal:
		return _processOrderPayment(ctx, order)
	case ordermgrpb.OrderType_Offline:
		fallthrough //nolint
	case ordermgrpb.OrderType_Airdrop:
		return _processFakeOrder(ctx, order)
	default:
		logger.Sugar().Errorw("processOrderPayment", "order", order.ID, "type", order.OrderType, "payment", order.PaymentID)
	}
	return nil
}

func processOrderPayments(ctx context.Context, orders []*ordermwpb.Order) {
	for _, order := range orders {
		if order.PaymentState != paymentmgrpb.PaymentState_Wait {
			continue
		}

		if err := processOrderPayment(ctx, order); err != nil {
			logger.Sugar().Errorw(
				"processOrderPayment",
				"Order", order.ID,
				"PaymentAccountID", order.PaymentAccountID,
				"PaymentState", order.PaymentState,
				"error", err,
			)
			continue
		}
	}
}

func orderPaidNotif(ctx context.Context, order *ordermwpb.Order) {
	coin, err := coinmwcli.GetCoin(ctx, order.PaymentCoinTypeID)
	if err != nil {
		logger.Sugar().Errorf("get coin failed when generate notif, err: %v", err)
		return
	}

	balanceAmount, err := decimal.NewFromString(order.PayWithBalanceAmount)
	if err != nil {
		logger.Sugar().Errorf("decial pay with balance amount err: %v", err)
		return
	}
	payAmount, err := decimal.NewFromString(order.PaymentAmount)
	if err != nil {
		logger.Sugar().Errorf("decimal pay amount err: %v", err)
		return
	}

	amount := balanceAmount.Add(payAmount)
	amountStr := amount.String()

	now := uint32(time.Now().Unix())
	_, err = notifmwcli.GenerateNotifs(ctx, &notif.GenerateNotifsRequest{
		AppID:     order.AppID,
		UserID:    order.UserID,
		EventType: basetypes.UsedFor_OrderCompleted,
		Vars: &template.TemplateVars{
			Amount:    &amountStr,
			CoinUnit:  &coin.Unit,
			Timestamp: &now,
		},
		NotifType: basetypes.NotifType_NotifUnicast,
	})

	if err != nil {
		logger.Sugar().Errorf("generate notif failed when order paid, err: %v", err)
	}
}

// TODO: use order middlware api
func checkOrderPayments(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			State: &commonpb.Uint32Val{Op: cruder.EQ, Value: uint32(ordermgrpb.OrderState_WaitPayment)},
		}, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("processOrderPayments", "offset", offset, "limit", limit, "error", err)
			return
		}
		if len(orders) == 0 {
			return
		}

		processOrderPayments(ctx, orders)

		offset += limit
	}
}
