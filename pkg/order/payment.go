//nolint:errcheck
package order

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	accountingmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/accounting"
	accountingmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/accounting"

	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordercli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"

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

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerv2mwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger/v2"
	ledgermwpkg "github.com/NpoolPlatform/ledger-middleware/pkg/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	eventmwcli "github.com/NpoolPlatform/inspire-middleware/pkg/client/event"
	eventmgrpb "github.com/NpoolPlatform/message/npool/inspire/mgr/v1/event"
	eventmwpb "github.com/NpoolPlatform/message/npool/inspire/mw/v1/event"

	"github.com/shopspring/decimal"
)

const TimeoutSeconds = 6 * 60 * 60

func processState(order *orderpb.Order, balance decimal.Decimal) (paymentmgrpb.PaymentState, ordermgrpb.OrderState) {
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

func processFinishAmount(order *orderpb.Order, balance decimal.Decimal) decimal.Decimal {
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

func processStock(order *orderpb.Order, balance decimal.Decimal) (unlocked, waitstart decimal.Decimal, err error) {
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
	order *orderpb.Order,
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
		_, err := ordercli.UpdateOrder(ctx, &orderpb.OrderReq{
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
	}

	switch newState {
	case paymentmgrpb.PaymentState_Done:
	case paymentmgrpb.PaymentState_Canceled:
	case paymentmgrpb.PaymentState_TimeOut:
	default:
		return nil
	}

	err := accountlock.Lock(order.PaymentAccountID)
	if err != nil {
		return err
	}
	defer func() {
		_ = accountlock.Unlock(order.PaymentAccountID)
	}()

	account, err := payaccmwcli.GetAccountOnly(ctx, &payaccmwpb.Conds{
		AccountID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.PaymentAccountID,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
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

func tryUpdatePaymentLedger(ctx context.Context, order *orderpb.Order) error {
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

func tryUpdateOrderLedger(ctx context.Context, order *orderpb.Order) error {
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

func unlockBalance(ctx context.Context, order *orderpb.Order, paymentState paymentmgrpb.PaymentState) error {
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
func _processOrderPayment(ctx context.Context, order *orderpb.Order) error {
	good, err := goodmwcli.GetGoodOnly(ctx, &goodmgrpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.AppID,
		},
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.GoodID,
		},
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
		AccountID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.PaymentAccountID,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
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
		good, err := goodmwcli.GetGoodOnly(ctx, &goodmgrpb.Conds{
			AppID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: order.AppID,
			},
			GoodID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: order.GoodID,
			},
		})
		if err != nil {
			return err
		}
		if good == nil {
			return fmt.Errorf("invalid good")
		}

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

	// TODO: move to TX end

	if state == paymentmgrpb.PaymentState_Done {
		good, err := goodmwcli.GetGoodOnly(ctx, &goodmgrpb.Conds{
			AppID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: order.AppID,
			},
			GoodID: &commonpb.StringVal{
				Op:    cruder.EQ,
				Value: order.GoodID,
			},
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

		units, err := decimal.NewFromString(order.Units)
		if err != nil {
			return err
		}
		goodValue := decimal.RequireFromString(good.Price).
			Mul(units).
			String()

		comms, err := accountingmwcli.Accounting(ctx, &accountingmwpb.AccountingRequest{
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
		})
		if err != nil {
			return err
		}

		details := []*ledgerdetailpb.DetailReq{}
		ioType := ledgerdetailpb.IOType_Incoming
		ioSubType := ledgerdetailpb.IOSubType_Commission

		for _, comm := range comms {
			ioExtra := fmt.Sprintf(
				`{"PaymentID":"%v","OrderID":"%v","DirectContributorID":"%v","OrderUserID":"%v"}`,
				order.PaymentID,
				order.ID,
				comm.GetDirectContributorUserID(),
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

		err = ledgerv2mwcli.BookKeeping(ctx, details)
		if err != nil {
			return err
		}

		event, err := eventmwcli.GetEventOnly(ctx, &eventmgrpb.Conds{
			AppID:  &basetypes.StringVal{Op: cruder.EQ, Value: order.AppID},
			GoodID: &basetypes.StringVal{Op: cruder.EQ, Value: order.GoodID},
		})
		if err != nil {
			return err
		}
		if event != nil {
			// TODO: get consecutive orders
			// TODO: get order good value
			_, err := eventmwcli.RewardEvent(ctx, &eventmwpb.RewardEventRequest{
				AppID:       order.AppID,
				UserID:      order.UserID,
				EventType:   event.EventType,
				GoodID:      &order.GoodID,
				Consecutive: 1,
				Amount:      "0",
			})
			if err != nil {
				logger.Sugar().Errorw("_processOrderPayment", "Error", err)
			}
			// TODO: add action credits to user
		}
	}

	switch state {
	case paymentmgrpb.PaymentState_Done:
		fallthrough //nolint
	case paymentmgrpb.PaymentState_Canceled:
		fallthrough //nolint
	case paymentmgrpb.PaymentState_TimeOut:
		return updateStock(ctx, order.GoodID, unlocked, decimal.NewFromInt(0), waitstart)
	}

	return nil
}

func _processFakeOrder(ctx context.Context, order *orderpb.Order) error {
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

	good, err := goodmwcli.GetGoodOnly(ctx, &goodmgrpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.AppID,
		},
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.GoodID,
		},
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

	_, err = accountingmwcli.Accounting(ctx, &accountingmwpb.AccountingRequest{
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
	})
	if err != nil {
		return err
	}

	return updateStock(ctx, order.GoodID, unlocked, decimal.NewFromInt(0), waitstart)
}

func processOrderPayment(ctx context.Context, order *orderpb.Order) error {
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

func processOrderPayments(ctx context.Context, orders []*orderpb.Order) {
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

// TODO: use order middlware api
func checkOrderPayments(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	for {
		orders, _, err := ordercli.GetOrders(ctx, &orderpb.Conds{
			State: &commonpb.Uint32Val{
				Op:    cruder.EQ,
				Value: uint32(ordermgrpb.OrderState_WaitPayment),
			},
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
