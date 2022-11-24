//nolint:errcheck
package order

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	"github.com/NpoolPlatform/message/npool"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordercli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"

	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	paymentmgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/payment"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgermwpkg "github.com/NpoolPlatform/ledger-middleware/pkg/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	archivement "github.com/NpoolPlatform/staker-manager/pkg/archivement"
	commission "github.com/NpoolPlatform/staker-manager/pkg/commission"

	"github.com/shopspring/decimal"
)

const TimeoutSeconds = 6 * 60 * 60

func processState(order *orderpb.Order, balance decimal.Decimal) (paymentmgrpb.PaymentState, ordermgrpb.OrderState) {
	if order.UserCanceled {
		return paymentmgrpb.PaymentState_Canceled, ordermgrpb.OrderState_Canceled
	}
	// TODO: use payment string amount to avoid float accuracy problem
	amount, _ := decimal.NewFromString(order.PaymentAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	if amount.Add(startAmount).Cmp(decimal.NewFromFloat(balance.InexactFloat64()+10e-7)) <= 0 {
		return paymentmgrpb.PaymentState_Done, ordermgrpb.OrderState_Paid
	}
	if order.CreatedAt+TimeoutSeconds < uint32(time.Now().Unix()) {
		return paymentmgrpb.PaymentState_TimeOut, ordermgrpb.OrderState_PaymentTimeout
	}
	return order.PaymentState, order.OrderState
}

func processFinishAmount(order *orderpb.Order, balance decimal.Decimal) (remain decimal.Decimal, finish string) {
	// TODO: use payment string amount to avoid float accuracy problem
	balanceStr := balance.String()

	finishAmount, _ := decimal.NewFromString(order.PaymentFinishAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	amount, _ := decimal.NewFromString(order.PaymentAmount)
	if order.UserCanceled {
		return finishAmount.Sub(startAmount), balanceStr
	}
	if amount.Add(startAmount).Cmp(decimal.NewFromFloat(balance.InexactFloat64()+10e-7)) <= 0 {
		remain := finishAmount.Sub(startAmount).Sub(amount)
		if remain.Cmp(decimal.NewFromInt(0)) <= 0 {
			remain = decimal.NewFromInt(0)
		}
		return remain, balanceStr
	}
	if order.CreatedAt+TimeoutSeconds < uint32(time.Now().Unix()) {
		return finishAmount.Sub(startAmount), balanceStr
	}
	return decimal.NewFromInt(0), balanceStr
}

func processStock(order *orderpb.Order, balance decimal.Decimal) (unlocked, inservice int32) {
	if order.UserCanceled {
		return int32(order.Units), 0
	}
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	amount, _ := decimal.NewFromString(order.PaymentAmount)
	if amount.Add(startAmount).Cmp(decimal.NewFromFloat(balance.InexactFloat64()+10e-7)) <= 0 {
		return int32(order.Units), int32(order.Units)
	}
	if order.CreatedAt+TimeoutSeconds < uint32(time.Now().Unix()) {
		return int32(order.Units), 0
	}
	return 0, 0
}

func trySavePaymentBalance(ctx context.Context, order *orderpb.Order, balance decimal.Decimal) error {
	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	paymentCoinUSDCurrency, _ := decimal.NewFromString(order.PaymentCoinUSDCurrency)
	paymentCoinUSDCurrencyF, _ := paymentCoinUSDCurrency.Float64()
	_, err := billingcli.CreatePaymentBalance(ctx, &billingpb.UserPaymentBalance{
		AppID:           order.AppID,
		UserID:          order.UserID,
		PaymentID:       order.ID,
		CoinTypeID:      order.PaymentCoinTypeID,
		Amount:          balance.InexactFloat64(),
		CoinUSDCurrency: paymentCoinUSDCurrencyF,
	})
	return err
}

func tryFinishPayment(ctx context.Context, order *orderpb.Order, newState paymentmgrpb.PaymentState, newOrderState ordermgrpb.OrderState, fakePayment bool, finishAmount string) error {
	if newState != order.PaymentState {
		logger.Sugar().Infow("tryFinishPayment", "payment", order.ID, "paymentState", order.PaymentState, "newState", newState, "paymentFinishAmount", finishAmount)
		_, err := ordercli.UpdateOrder(ctx, &orderpb.OrderReq{
			ID:                  &order.ID,
			PaymentState:        &newState,
			FakePayment:         &fakePayment,
			State:               &newOrderState,
			PaymentID:           &order.PaymentID,
			PaymentFinishAmount: &finishAmount,
		})
		if err != nil {
			return err
		}
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
		accountlock.Unlock(order.PaymentAccountID)
	}()

	goodPayment, err := billingcli.GetAccountGoodPayment(ctx, order.PaymentAccountID)
	if err != nil {
		return err
	}
	if goodPayment == nil {
		return fmt.Errorf("invalid account good payment")
	}

	goodPayment.Idle = true
	goodPayment.OccupiedBy = billingconst.TransactionForNotUsed
	goodPayment.UsedFor = billingconst.TransactionForNotUsed

	_, err = billingcli.UpdateGoodPayment(ctx, goodPayment)
	return err
}

func tryUpdatePaymentLedger(ctx context.Context, order *orderpb.Order) error {
	finishAmount, _ := decimal.NewFromString(order.PaymentFinishAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
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

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, order.ID, order.ID)
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

func unlockBalance(ctx context.Context, order *orderpb.Order) error {
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
		order.ID, order.ID, order.PayWithBalanceAmount)

	switch order.PaymentState {
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
	coin, err := coininfocli.GetCoinInfo(ctx, order.PaymentCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coininfo")
	}

	account, err := billingcli.GetAccount(ctx, order.PaymentAccountID)
	if err != nil {
		return err
	}
	if account == nil {
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
	remain, finishAmount := processFinishAmount(order, bal)
	unlocked, inservice := processStock(order, bal)

	amount, _ := decimal.NewFromString(order.PaymentAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	dueAmount := amount.Add(startAmount).String()
	logger.Sugar().Infow("processOrderPayment", "order", order.ID, "payment",
		order.ID, "coin", coin.Name, "startAmount", order.PaymentStartAmount,
		"finishAmount", order.PaymentFinishAmount, "amount", order.PaymentAmount,
		"dueAmount", dueAmount, "paymentState", order.PaymentState,
		"newState", state, "newOrderState", newOrderState, "remain", remain, "unlocked", unlocked,
		"inservice", inservice, "balance", bal,
		"coin", coin.Name, "address", account.Address, "balance", balance,
	)

	if state == paymentmgrpb.PaymentState_Done {
		good, err := goodscli.GetGood(ctx, order.GoodID)
		if err != nil {
			return err
		}

		_, err = ledgermwpkg.TryCreateProfit(ctx, order.AppID, order.UserID, good.CoinTypeID)
		if err != nil {
			return err
		}
	}

	if err := trySavePaymentBalance(ctx, order, remain); err != nil {
		return err
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

	if err := unlockBalance(ctx, order); err != nil {
		return err
	}

	// TODO: move to TX end

	if order.PaymentState == paymentmgrpb.PaymentState_Done {
		if err := commission.CalculateCommission(ctx, order.ID, false); err != nil {
			return err
		}
		if err := archivement.CalculateArchivement(ctx, order.ID); err != nil {
			return err
		}
	}

	switch state {
	case paymentmgrpb.PaymentState_Done:
		fallthrough //nolint
	case paymentmgrpb.PaymentState_Canceled:
		fallthrough //nolint
	case paymentmgrpb.PaymentState_TimeOut:
		return updateStock(ctx, order.GoodID, unlocked, inservice)
	}

	return nil
}

func _processFakeOrder(ctx context.Context, order *orderpb.Order) error {
	coin, err := coininfocli.GetCoinInfo(ctx, order.PaymentCoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coininfo")
	}

	state := paymentmgrpb.PaymentState_Done
	orderState := ordermgrpb.OrderState_Paid
	unlocked, inservice := int32(order.Units), int32(order.Units)

	amount, _ := decimal.NewFromString(order.PaymentAmount)
	startAmount, _ := decimal.NewFromString(order.PaymentStartAmount)
	dueAmount := amount.Add(startAmount).String()
	logger.Sugar().Infow("processFakeOrder", "order", order.ID, "payment",
		order.ID, "coin", coin.Name, "startAmount", order.PaymentStartAmount,
		"finishAmount", order.PaymentFinishAmount, "amount", order.PaymentAmount,
		"dueAmount", dueAmount, "state", order.PaymentState,
		"newState", state, "newOrderState", orderState, "unlocked", unlocked, "inservice", inservice)

	if err := tryFinishPayment(ctx, order, state, orderState, true, order.PaymentStartAmount); err != nil {
		return err
	}

	if err := archivement.CalculateArchivement(ctx, order.ID); err != nil {
		return err
	}

	return updateStock(ctx, order.GoodID, unlocked, inservice)
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
			logger.Sugar().Errorw("processOrderPayment", "error", err)
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
			State: &npool.Uint32Val{
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
