package order

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"
	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order/order"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	archivement "github.com/NpoolPlatform/staker-manager/pkg/archivement"
	commission "github.com/NpoolPlatform/staker-manager/pkg/commission"

	"github.com/shopspring/decimal"
)

func processState(payment *orderpb.Payment, balance decimal.Decimal) string {
	if payment.UserSetCanceled {
		return orderconst.PaymentStateCanceled
	}
	// TODO: use payment string amount to avoid float accuracy problem
	if payment.Amount+payment.StartAmount <= balance.InexactFloat64() {
		return orderconst.PaymentStateDone
	}
	if payment.CreateAt+orderconst.TimeoutSeconds < uint32(time.Now().Unix()) {
		return orderconst.PaymentStateTimeout
	}
	return payment.State
}

func processFinishAmount(payment *orderpb.Payment, balance decimal.Decimal) decimal.Decimal {
	// TODO: use payment string amount to avoid float accuracy problem
	payment.FinishAmount = balance.InexactFloat64()

	if payment.UserSetCanceled {
		return decimal.NewFromFloat(payment.FinishAmount).Sub(decimal.NewFromFloat(payment.StartAmount))
	}
	if payment.Amount+payment.StartAmount <= balance.InexactFloat64()+10e-7 {
		remain := decimal.NewFromFloat(payment.FinishAmount).
			Sub(decimal.NewFromFloat(payment.StartAmount)).
			Sub(decimal.NewFromFloat(payment.Amount))
		if remain.Cmp(decimal.NewFromInt(0)) <= 0 {
			remain = decimal.NewFromInt(0)
		}
		return remain
	}
	if payment.CreateAt+orderconst.TimeoutSeconds < uint32(time.Now().Unix()) {
		return decimal.NewFromFloat(payment.FinishAmount).Sub(decimal.NewFromFloat(payment.StartAmount))
	}
	return decimal.NewFromInt(0)
}

func processStock(order *orderpb.Order, payment *orderpb.Payment, balance decimal.Decimal) (unlocked, inservice int32) {
	if payment.UserSetCanceled {
		return int32(order.Units), 0
	}
	if payment.Amount+payment.StartAmount <= balance.InexactFloat64()+10e-4 {
		return int32(order.Units), int32(order.Units)
	}
	if payment.CreateAt+orderconst.TimeoutSeconds < uint32(time.Now().Unix()) {
		return int32(order.Units), 0
	}
	return 0, 0
}

func trySavePaymentBalance(ctx context.Context, payment *orderpb.Payment, balance decimal.Decimal) error {
	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	_, err := billingcli.CreatePaymentBalance(ctx, &billingpb.UserPaymentBalance{
		AppID:           payment.AppID,
		UserID:          payment.UserID,
		PaymentID:       payment.ID,
		CoinTypeID:      payment.CoinInfoID,
		Amount:          balance.InexactFloat64(),
		CoinUSDCurrency: payment.CoinUSDCurrency,
	})
	return err
}

func tryFinishPayment(ctx context.Context, payment *orderpb.Payment, newState string) error {
	if newState != payment.State {
		logger.Sugar().Infow("tryFinishPayment", "payment", payment.ID, "state", payment.State, "newState", newState)
		payment.State = newState
		_, err := ordercli.UpdatePayment(ctx, payment)
		if err != nil {
			return err
		}
	}

	switch newState {
	case orderconst.PaymentStateDone:
	case orderconst.PaymentStateCanceled:
	default:
		return nil
	}

	err := accountlock.Lock(payment.AccountID)
	if err != nil {
		return err
	}
	defer func() {
		accountlock.Unlock(payment.AccountID) //nolint
	}()

	goodPayment, err := billingcli.GetAccountGoodPayment(ctx, payment.AccountID)
	if err != nil {
		return err
	}
	if goodPayment == nil {
		return fmt.Errorf("invalid account good payment")
	}

	goodPayment.Idle = true
	goodPayment.OccupiedBy = ""

	_, err = billingcli.UpdateGoodPayment(ctx, goodPayment)
	return err
}

func tryUpdatePaymentLedger(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	if payment.FinishAmount <= payment.StartAmount {
		return nil
	}

	switch payment.State {
	case orderconst.PaymentStateDone:
	case orderconst.PaymentStateCanceled:
	case orderconst.PaymentStateTimeout:
	default:
		return nil
	}

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, payment.ID, order.ID)
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Payment
	amount := decimal.NewFromFloat(payment.FinishAmount).
		Sub(decimal.NewFromFloat(payment.StartAmount)).
		String()

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &payment.AppID,
		UserID:     &payment.UserID,
		CoinTypeID: &payment.CoinInfoID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
}

func tryUpdateOrderLedger(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	if payment.Amount <= 0 {
		return nil
	}

	switch payment.State {
	case orderconst.PaymentStateDone:
	default:
		return nil
	}

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, payment.ID, order.ID)
	amount := fmt.Sprintf("%v", payment.Amount)
	ioType := ledgerdetailpb.IOType_Outcoming
	ioSubType := ledgerdetailpb.IOSubType_Payment

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &payment.AppID,
		UserID:     &payment.UserID,
		CoinTypeID: &payment.CoinInfoID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
}

func unlockBalance(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	if payment.PayWithBalanceAmount == "" {
		return nil
	}

	balance, err := decimal.NewFromString(payment.PayWithBalanceAmount)
	if err != nil {
		return err
	}

	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	outcoming := decimal.NewFromInt(0)
	unlocked, err := decimal.NewFromString(payment.PayWithBalanceAmount)
	if err != nil {
		return err
	}

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v", "BalanceAmount": "%v"}`,
		payment.ID, order.ID, payment.PayWithBalanceAmount)

	switch payment.State {
	case orderconst.PaymentStateCanceled:
	case orderconst.PaymentStateTimeout:
	case orderconst.PaymentStateDone:
		outcoming = unlocked
	default:
		return nil
	}

	return ledgermwcli.UnlockBalance(
		ctx,
		payment.AppID, payment.UserID, payment.CoinInfoID,
		ledgerdetailpb.IOSubType_Payment,
		unlocked, outcoming,
		ioExtra,
	)
}

func _processOrderPayment(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	coin, err := coininfocli.GetCoinInfo(ctx, payment.CoinInfoID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coininfo")
	}

	account, err := billingcli.GetAccount(ctx, payment.AccountID)
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

	state := processState(payment, bal)
	remain := processFinishAmount(payment, bal)
	unlocked, inservice := processStock(order, payment, bal)

	logger.Sugar().Infow("processOrderPayment", "order", order.ID, "payment",
		payment.ID, "coin", coin.Name, "startAmount", payment.StartAmount,
		"finishAmount", payment.FinishAmount, "amount", payment.Amount,
		"dueAmount", payment.Amount+payment.StartAmount, "state", payment.State,
		"newState", state, "remain", remain, "unlocked", unlocked,
		"inservice", inservice, "balance", bal,
		"coin", coin.Name, "address", account.Address, "balance", balance,
	)

	if err := trySavePaymentBalance(ctx, payment, remain); err != nil {
		return err
	}

	if err := tryFinishPayment(ctx, payment, state); err != nil {
		return err
	}

	// TODO: move to TX

	if err := tryUpdatePaymentLedger(ctx, order, payment); err != nil {
		return err
	}

	if err := tryUpdateOrderLedger(ctx, order, payment); err != nil {
		return err
	}

	if payment.State == orderconst.PaymentStateDone {
		if err := commission.CalculateCommission(ctx, order.ID); err != nil {
			return err
		}
		if err := archivement.CalculateArchivement(ctx, order.ID); err != nil {
			return err
		}
	}

	if err := unlockBalance(ctx, order, payment); err != nil {
		return err
	}

	return updateStock(ctx, order.GoodID, unlocked, inservice)
}

func _processFakeOrder(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	coin, err := coininfocli.GetCoinInfo(ctx, payment.CoinInfoID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coininfo")
	}

	state := orderconst.PaymentStateDone
	unlocked, inservice := int32(order.Units), int32(order.Units)
	payment.FakePayment = true
	payment.FinishAmount = payment.StartAmount

	logger.Sugar().Infow("processFakeOrder", "order", order.ID, "payment",
		payment.ID, "coin", coin.Name, "startAmount", payment.StartAmount,
		"finishAmount", payment.FinishAmount, "amount", payment.Amount,
		"dueAmount", payment.Amount+payment.StartAmount, "state", payment.State,
		"newState", state, "unlocked", unlocked, "inservice", inservice)

	if err := tryFinishPayment(ctx, payment, state); err != nil {
		return err
	}

	return updateStock(ctx, order.GoodID, unlocked, inservice)
}

func processOrderPayment(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	switch order.OrderType {
	case orderconst.OrderTypeNormal:
		fallthrough //nolint
	case ordermgrpb.OrderType_Normal.String():
		return _processOrderPayment(ctx, order, payment)
	case orderconst.OrderTypeOffline:
		fallthrough //nolint
	case ordermgrpb.OrderType_Offline.String():
		fallthrough //nolint
	case orderconst.OrderTypeAirdrop:
		fallthrough //nolint
	case ordermgrpb.OrderType_Airdrop.String():
		return _processFakeOrder(ctx, order, payment)
	default:
		logger.Sugar().Errorw("processOrderPayment", "order", order.ID, "type", order.OrderType, "payment", payment.ID)
	}
	return nil
}

func processOrderPayments(ctx context.Context, orders []*orderpb.Order) error {
	for _, order := range orders {
		payment, err := ordercli.GetOrderPayment(ctx, order.ID)
		if err != nil {
			logger.Sugar().Infow("processOrderPayments", "OrderID", order.ID, "error", err)
			return err
		}
		if payment == nil {
			continue
		}
		if payment.State != orderconst.PaymentStateWait {
			continue
		}

		if err := processOrderPayment(ctx, order, payment); err != nil {
			return err
		}
	}
	return nil
}

func checkOrderPayments(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	for {
		orders, err := ordercli.GetOrders(ctx, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("processOrderPayments", "offset", offset, "limit", limit, "error", err)
			return
		}
		if len(orders) == 0 {
			return
		}

		err = processOrderPayments(ctx, orders)
		if err != nil {
			logger.Sugar().Errorw("processOrderPayments", "offset", offset, "limit", limit, "error", err)
			return
		}

		offset += limit
	}
}
