package payment

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	accountlock "github.com/NpoolPlatform/staker-manager/pkg/accountlock"

	stockcli "github.com/NpoolPlatform/stock-manager/pkg/client"
	stockconst "github.com/NpoolPlatform/stock-manager/pkg/const"

	ledgerdetailcli "github.com/NpoolPlatform/ledger-manager/pkg/client/detail"
	ledgergeneralcli "github.com/NpoolPlatform/ledger-manager/pkg/client/general"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledgermgr/detail"
	ledgergeneralpb "github.com/NpoolPlatform/message/npool/ledgermgr/general"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/shopspring/decimal"
)

func processState(payment *orderpb.Payment, balance decimal.Decimal) string {
	if payment.UserSetCanceled {
		return orderconst.PaymentStateCanceled
	}
	// TODO: use payment string amount to avoid float accuracy problem
	if payment.Amount+payment.StartAmount <= balance.InexactFloat64()+10e-4 {
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
	if payment.Amount+payment.StartAmount <= balance.InexactFloat64()+10e-4 {
		return decimal.NewFromFloat(payment.FinishAmount).
			Sub(decimal.NewFromFloat(payment.StartAmount)).
			Sub(decimal.NewFromFloat(payment.Amount))
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

	goodPayment, err := billingcli.GetAccountGoodPayment(ctx, payment.AccountID)
	if err != nil {
		return err
	}
	if goodPayment == nil {
		return fmt.Errorf("invalid account good payment")
	}

	goodPayment.Idle = true
	goodPayment.OccupiedBy = ""

	err = accountlock.Lock(payment.AccountID)
	if err != nil {
		return err
	}
	defer func() {
		if err := accountlock.Unlock(payment.AccountID); err != nil {
			logger.Sugar().Warnw("tryFinishPayment", "account", payment.AccountID)
		}
	}()

	_, err = billingcli.UpdateGoodPayment(ctx, goodPayment)
	return err
}

func updateStock(ctx context.Context, order *orderpb.Order, unlocked, inservice int32) error {
	stock, err := stockcli.GetStockOnly(ctx, cruder.NewFilterConds().
		WithCond(stockconst.StockFieldGoodID, cruder.EQ, structpb.NewStringValue(order.GoodID)))
	if err != nil {
		return err
	}
	if stock == nil {
		return fmt.Errorf("invalid stock")
	}

	fields := cruder.NewFilterFields()
	if inservice > 0 {
		fields = fields.WithField(stockconst.StockFieldInService, structpb.NewNumberValue(float64(inservice)))
	}
	if unlocked > 0 {
		fields = fields.WithField(stockconst.StockFieldLocked, structpb.NewNumberValue(float64(unlocked*-1)))
	}

	if len(fields) == 0 {
		return nil
	}

	logger.Sugar().Infow("updateStock", "good", order.GoodID, "inservice", inservice, "unlocked", unlocked)

	_, err = stockcli.AddStockFields(ctx, stock.ID, fields)
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

	general, err := ledgergeneralcli.GetGeneralOnly(ctx, &ledgergeneralpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: payment.AppID,
		},
		UserID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: payment.UserID,
		},
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: payment.CoinInfoID,
		},
	})
	if err != nil {
		return err
	}

	if general == nil {
		_, err = ledgergeneralcli.CreateGeneral(ctx, &ledgergeneralpb.GeneralReq{
			AppID:      &payment.AppID,
			UserID:     &payment.UserID,
			CoinTypeID: &payment.CoinInfoID,
		})
		if err != nil {
			return nil
		}
	}

	incoming := fmt.Sprintf("%v", payment.FinishAmount-payment.StartAmount)

	_, err = ledgergeneralcli.AddGeneral(ctx, &ledgergeneralpb.GeneralReq{
		Incoming:  &incoming,
		Spendable: &incoming,
	})
	if err != nil {
		return err
	}

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, payment.ID, order.ID)
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Payment

	_, err = ledgerdetailcli.CreateDetail(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &payment.AppID,
		UserID:     &payment.UserID,
		CoinTypeID: &payment.CoinInfoID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &incoming,
		IOExtra:    &ioExtra,
	})
	return err
}

func tryUpdateOrderLedger(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, payment.ID, order.ID)
	amount := fmt.Sprintf("%v", payment.Amount)
	spendable := fmt.Sprintf("-%v", payment.Amount)
	ioType := ledgerdetailpb.IOType_Outcoming
	ioSubType := ledgerdetailpb.IOSubType_Payment

	_, err := ledgerdetailcli.CreateDetail(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &payment.AppID,
		UserID:     &payment.UserID,
		CoinTypeID: &payment.CoinInfoID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
	if err != nil {
		return err
	}

	_, err = ledgergeneralcli.AddGeneral(ctx, &ledgergeneralpb.GeneralReq{
		Outcoming: &amount,
		Spendable: &spendable,
	})

	return err
}

func _processOrder(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
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

	logger.Sugar().Infow("processOrder", "order", order.ID, "payment",
		payment.ID, "coin", coin.Name, "startAmount", payment.StartAmount,
		"finishAmount", payment.FinishAmount, "amount", payment.Amount,
		"dueAmount", payment.Amount+payment.StartAmount, "state", payment.State,
		"newState", state, "remain", remain, "unlocked", unlocked,
		"inservice", inservice, "balance", bal)

	if err := trySavePaymentBalance(ctx, payment, remain); err != nil {
		return err
	}

	if err := tryFinishPayment(ctx, payment, state); err != nil {
		return err
	}

	if err := tryUpdatePaymentLedger(ctx, order, payment); err != nil {
		return err
	}
	if err := tryUpdateOrderLedger(ctx, order, payment); err != nil {
		return err
	}

	// TODO: update inviters' commission ledger

	return updateStock(ctx, order, unlocked, inservice)
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

	return updateStock(ctx, order, unlocked, inservice)
}

func processOrder(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	switch order.OrderType {
	case orderconst.OrderTypeNormal:
		return _processOrder(ctx, order, payment)
	case orderconst.OrderTypeOffline:
		fallthrough //nolint
	case orderconst.OrderTypeAirdrop:
		return _processFakeOrder(ctx, order, payment)
	default:
		logger.Sugar().Errorw("processOrder", "order", order.ID, "type", order.OrderType, "payment", payment.ID)
	}
	return nil
}

func processOrders(ctx context.Context, orders []*orderpb.Order) error {
	for _, order := range orders {
		payment, err := ordercli.GetOrderPayment(ctx, order.ID)
		if err != nil {
			return fmt.Errorf("fail get order payment: %v", err)
		}
		if payment == nil {
			continue
		}
		if payment.State != orderconst.PaymentStateWait {
			continue
		}

		if err := processOrder(ctx, order, payment); err != nil {
			return fmt.Errorf("fail process order: %v", err)
		}
	}
	return nil
}

func checkOrders(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	offset := int32(0)
	limit := int32(1000)

	for {
		orders, err := ordercli.GetOrders(ctx, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("getOrders", "offset", offset, "limit", limit)
			return
		}
		if len(orders) == 0 {
			return
		}

		err = processOrders(ctx, orders)
		if err != nil {
			logger.Sugar().Errorw("processOrders", "offset", offset, "limit", limit)
			return
		}

		offset += limit
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		checkOrders(ctx)
	}
}
