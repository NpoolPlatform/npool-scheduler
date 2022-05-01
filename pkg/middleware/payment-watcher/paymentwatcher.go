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

	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

func watchPaymentState(ctx context.Context) { //nolint
	payments, err := grpc2.GetPaymentsByState(ctx, &orderpb.GetPaymentsByStateRequest{
		State: orderconst.PaymentStateWait,
	})
	if err != nil {
		logger.Sugar().Errorf("fail to get wait payments: %v", err)
		return
	}

	for _, pay := range payments {
		coinInfo, err := grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
			ID: pay.CoinInfoID,
		})
		if err != nil || coinInfo == nil {
			logger.Sugar().Errorf("fail to get coin %v info: %v", pay.CoinInfoID, err)
			continue
		}

		account, err := grpc2.GetBillingAccount(ctx, &billingpb.GetCoinAccountRequest{
			ID: pay.AccountID,
		})
		if err != nil {
			logger.Sugar().Errorf("fail to get payment account: %v", err)
			continue
		}
		if account == nil {
			logger.Sugar().Errorf("fail to get payment account")
			continue
		}

		balance, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
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
			pay.ID, coinInfo.Name, balance.Balance, pay.StartAmount, pay.Amount)

		newState := pay.State
		if balance.Balance-pay.StartAmount >= pay.Amount {
			newState = orderconst.PaymentStateDone
			pay.FinishAmount = balance.Balance

			myAmount := balance.Balance - pay.StartAmount - pay.Amount
			if myAmount > 0 {
				_, err := grpc2.CreateUserPaymentBalance(ctx, &billingpb.CreateUserPaymentBalanceRequest{
					Info: &billingpb.UserPaymentBalance{
						AppID:     pay.AppID,
						UserID:    pay.UserID,
						PaymentID: pay.ID,
						Amount:    myAmount,
					},
				})
				if err != nil {
					logger.Sugar().Errorf("fail create user payment balance for payment %v: %v", pay.ID, err)
				}
			}
		}
		if pay.CreateAt+orderconst.TimeoutSeconds < uint32(time.Now().Unix()) {
			newState = orderconst.PaymentStateTimeout
			pay.FinishAmount = balance.Balance
		}

		if newState != pay.State {
			logger.Sugar().Infof("payment %v try %v -> %v", pay.ID, pay.State, newState)
			pay.State = newState
			_, err := grpc2.UpdatePayment(ctx, &orderpb.UpdatePaymentRequest{
				Info: pay,
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
	}
}

func releasePaymentAccount(ctx context.Context, payment *billingpb.GoodPayment) {
	go func() {
		for {
			err := accountlock.Lock(payment.AccountID)
			if err != nil {
				time.Sleep(10 * time.Second)
				continue
			}

			payment.Idle = true
			payment.OccupiedBy = ""

			_, err = grpc2.UpdateGoodPayment(ctx, &billingpb.UpdateGoodPaymentRequest{
				Info: payment,
			})
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
	defer releasePaymentAccount(ctx, payment)

	payment.Idle = false
	payment.OccupiedBy = "collecting"
	_, err = grpc2.UpdateGoodPayment(ctx, &billingpb.UpdateGoodPaymentRequest{
		Info: payment,
	})
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

	coinLimit := 0

	coinsetting, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
		CoinTypeID: coinInfo.ID,
	})
	if err != nil || coinsetting == nil {
		return xerrors.Errorf("fail get coin setting: %v", err)
	}

	coinLimit = int(coinsetting.PaymentAccountCoinAmount)
	logger.Sugar().Infof("balance %v coin limit %v reserved %v of payment %v",
		balance.Balance, coinLimit, coinInfo.ReservedAmount, payment.AccountID)

	if int(balance.Balance) <= coinLimit || balance.Balance <= coinInfo.ReservedAmount {
		err = accountlock.Unlock(payment.AccountID)
		if err != nil {
			return xerrors.Errorf("fail unlock account %v: %v", payment.AccountID, err)
		}
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
