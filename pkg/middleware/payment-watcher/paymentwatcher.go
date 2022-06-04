package paymentwatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	grpc2 "github.com/NpoolPlatform/staker-manager/pkg/grpc"
	accountlock "github.com/NpoolPlatform/staker-manager/pkg/middleware/account"

	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"
	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	"google.golang.org/protobuf/types/known/structpb"

	stockcli "github.com/NpoolPlatform/stock-manager/pkg/client"
	stockconst "github.com/NpoolPlatform/stock-manager/pkg/const"

	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

func watchPaymentState(ctx context.Context) { //nolint
	offset := int32(0)
	limit := int32(1000)

	for {
		orders, err := grpc2.GetOrders(ctx, &orderpb.GetOrdersRequest{
			Offset: offset,
			Limit:  limit,
		})
		if err != nil {
			logger.Sugar().Errorf("fail get orders offset %v: %v", offset, limit)
			return
		}
		if len(orders) == 0 {
			return
		}

		for _, order := range orders {
			payment, err := grpc2.GetPaymentByOrder(ctx, &orderpb.GetPaymentByOrderRequest{
				OrderID: order.ID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail get order %v payment: %v", order.ID, err)
				return
			}

			if payment == nil {
				// TODO: process order without payment
				continue
			}

			if payment.State != orderconst.PaymentStateWait {
				continue
			}

			unLocked := int32(0)
			inService := int32(0)
			myAmount := float64(0)
			var coinInfo *coininfopb.CoinInfo
			var account *billingpb.CoinAccountInfo
			var balance *sphinxproxypb.BalanceInfo

			if payment == nil {
				// TODO: process order without payment
				continue
			}

			coinInfo, err = grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
				ID: payment.CoinInfoID,
			})
			if err != nil || coinInfo == nil {
				logger.Sugar().Errorf("fail to get coin %v info: %v", payment.CoinInfoID, err)
				continue
			}

			account, err = grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
				ID: payment.AccountID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail to get payment account: %v", err)
				continue
			}
			if account == nil {
				logger.Sugar().Errorf("fail to get payment account")
				continue
			}

			balance, err = grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
				Name:    coinInfo.Name,
				Address: account.Address,
			})
			if err != nil {
				logger.Sugar().Errorf("fail to get wallet balance: %v", err)
				continue
			}
			if balance == nil {
				logger.Sugar().Errorf("fail to get wallet balance")
				continue
			}

			logger.Sugar().Infof("payment %v checking coin %v balance %v start amount %v pay amount %v",
				payment.ID, coinInfo.Name, balance.Balance, payment.StartAmount, payment.Amount)

			newState := payment.State
			if payment.UserSetCanceled {
				newState = orderconst.PaymentStateCanceled
				payment.FinishAmount = balance.Balance

				unLocked += int32(order.Units)

				myAmount = balance.Balance - payment.StartAmount
			} else if balance.Balance-payment.StartAmount >= payment.Amount {
				newState = orderconst.PaymentStateDone
				payment.FinishAmount = balance.Balance

				unLocked += int32(order.Units)
				inService += int32(order.Units)

				myAmount = balance.Balance - payment.StartAmount - payment.Amount
			} else if payment.CreateAt+orderconst.TimeoutSeconds < uint32(time.Now().Unix()) {
				newState = orderconst.PaymentStateTimeout
				payment.FinishAmount = balance.Balance

				unLocked += int32(order.Units)

				myAmount = balance.Balance - payment.StartAmount
			}

			if myAmount > 0 {
				_, err := grpc2.CreateUserPaymentBalance(ctx, &billingpb.CreateUserPaymentBalanceRequest{
					Info: &billingpb.UserPaymentBalance{
						AppID:     payment.AppID,
						UserID:    payment.UserID,
						PaymentID: payment.ID,
						Amount:    myAmount,
					},
				})
				if err != nil {
					logger.Sugar().Errorf("fail create user payment balance for payment %v: %v", payment.ID, err)
				}
			}

			if newState != payment.State {
				logger.Sugar().Infof("payment %v try %v -> %v", payment.ID, payment.State, newState)
				payment.State = newState
				_, err := grpc2.UpdatePayment(ctx, &orderpb.UpdatePaymentRequest{
					Info: payment,
				})
				if err != nil {
					logger.Sugar().Errorf("fail to update payment state: %v", err)
					continue
				}
			}

			if newState == orderconst.PaymentStateDone {
				myPayment, err := grpc2.GetGoodPaymentByAccount(ctx, &billingpb.GetGoodPaymentByAccountRequest{
					AccountID: account.ID,
				})
				if err != nil {
					logger.Sugar().Errorf("fail to get good payment: %v", err)
					continue
				}
				if myPayment == nil {
					logger.Sugar().Errorf("fail to get good payment")
					continue
				}

				myPayment.Idle = true
				myPayment.OccupiedBy = ""

				_, err = grpc2.UpdateGoodPayment(ctx, &billingpb.UpdateGoodPaymentRequest{
					Info: myPayment,
				})
				if err != nil {
					logger.Sugar().Errorf("fail to update good payment: %v", err)
				}

				err = accountlock.Unlock(account.ID)
				if err != nil {
					logger.Sugar().Errorf("fail unlock %v: %v", account.ID, err)
				}
			}

			stock, err := stockcli.GetStockOnly(ctx, cruder.NewFilterConds().
				WithCond(stockconst.StockFieldGoodID, cruder.EQ, structpb.NewStringValue(order.GoodID)))
			if err != nil || stock == nil {
				logger.Sugar().Errorf("fail get good stock: %v", err)
				continue
			}

			fields := cruder.NewFilterFields()
			if inService > 0 {
				fields = fields.WithField(stockconst.StockFieldInService, structpb.NewNumberValue(float64(inService)))
			}
			if unLocked > 0 {
				fields = fields.WithField(stockconst.StockFieldLocked, structpb.NewNumberValue(float64(unLocked*-1)))
			}

			if len(fields) > 0 {
				logger.Sugar().Infof("update good %v stock in service %v unlocked %v (%v)", order.GoodID, inService, unLocked, newState)
				_, err = stockcli.AddStockFields(ctx, stock.ID, fields)
				if err != nil {
					logger.Sugar().Errorf("fail add good in service: %v", err)
					continue
				}
			}
		}
		offset += limit
	}
}

func setPaymentAccountIdle(ctx context.Context, payment *billingpb.GoodPayment, idle bool, occupiedBy string) error {
	payment.Idle = idle
	payment.OccupiedBy = occupiedBy

	_, err := grpc2.UpdateGoodPayment(ctx, &billingpb.UpdateGoodPaymentRequest{
		Info: payment,
	})
	if err != nil {
		return xerrors.Errorf("fail to update good payment: %v", err)
	}

	return nil
}

func releasePaymentAccount(ctx context.Context, payment *billingpb.GoodPayment, err error) {
	go func() {
		for {
			if err == nil {
				err = accountlock.Lock(payment.AccountID)
				if err != nil {
					time.Sleep(10 * time.Second)
					continue
				}
			}

			err = setPaymentAccountIdle(ctx, payment, true, "")
			if err != nil {
				logger.Sugar().Errorf("fail to update good payment: %v", err)
			}

			err = accountlock.Unlock(payment.AccountID)
			if err != nil {
				logger.Sugar().Warnf("fail unlock account %v: %v", payment.AccountID, err)
			}

			break
		}
	}()
}

func checkAndTransfer(ctx context.Context, payment *billingpb.GoodPayment, coinInfo *coininfopb.CoinInfo) error { //nolint
	err := accountlock.Lock(payment.AccountID)
	if err != nil {
		return xerrors.Errorf("fail lock account: %v", err)
	}
	defer releasePaymentAccount(ctx, payment, err)

	err = setPaymentAccountIdle(ctx, payment, false, "collecting")
	if err != nil {
		return xerrors.Errorf("fail to update good payment: %v", err)
	}

	account, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
		ID: payment.AccountID,
	})
	if err != nil || account == nil {
		return xerrors.Errorf("fail get account: %v", err)
	}

	balance, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coinInfo.Name,
		Address: account.Address,
	})
	if err != nil {
		return xerrors.Errorf("fail get wallet balance of %v %v: %v", coinInfo.Name, account.Address, err)
	}

	coinLimit := 0.0

	coinsetting, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
		CoinTypeID: coinInfo.ID,
	})
	if err != nil || coinsetting == nil {
		return xerrors.Errorf("fail get coin setting: %v", err)
	}

	coinLimit = coinsetting.PaymentAccountCoinAmount
	logger.Sugar().Infof("balance %v coin limit %v reserved %v of payment %v",
		balance.Balance, coinLimit, coinInfo.ReservedAmount, payment.AccountID)

	if balance.Balance <= coinLimit || balance.Balance <= coinInfo.ReservedAmount {
		return nil
	}

	// Here we just create transaction, watcher will process it
	_, err = grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
		Info: &billingpb.CoinAccountTransaction{
			AppID:              uuid.UUID{}.String(),
			UserID:             uuid.UUID{}.String(),
			GoodID:             uuid.UUID{}.String(),
			FromAddressID:      payment.AccountID,
			ToAddressID:        coinsetting.GoodIncomingAccountID,
			CoinTypeID:         coinInfo.ID,
			Amount:             balance.Balance - coinInfo.ReservedAmount,
			Message:            fmt.Sprintf("payment collecting transfer of %v at %v", payment.GoodID, time.Now()),
			ChainTransactionID: uuid.New().String(),
		},
	})
	if err != nil {
		return xerrors.Errorf("fail create transaction of %v: %v", payment.AccountID, err)
	}

	return nil
}

func watchPaymentAmount(ctx context.Context) {
	payments, err := grpc2.GetGoodPayments(ctx, &billingpb.GetGoodPaymentsRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get good payments: %v", err)
		return
	}

	coins := map[string]*coininfopb.CoinInfo{}

	for _, payment := range payments {
		if !payment.Idle {
			continue
		}

		coinInfo, ok := coins[payment.PaymentCoinTypeID]
		if !ok {
			coinInfo, err = grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
				ID: payment.PaymentCoinTypeID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail get coin info: %v", err)
				continue
			}
			coins[payment.PaymentCoinTypeID] = coinInfo
		}

		err = checkAndTransfer(ctx, payment, coinInfo)
		if err != nil {
			logger.Sugar().Errorf("fail check and transfer: %v", err)
		}
	}
}

func Watch(ctx context.Context) {
	payticker := time.NewTicker(30 * time.Second)
	collectticker := time.NewTicker(1 * time.Minute)

	for { //nolint
		select {
		case <-payticker.C:
			watchPaymentState(ctx)
		case <-collectticker.C:
			watchPaymentAmount(ctx)
		}
	}
}
