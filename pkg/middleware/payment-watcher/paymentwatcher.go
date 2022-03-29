package paymentwatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"

	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"
	currency "github.com/NpoolPlatform/cloud-hashing-staker/pkg/middleware/currency"

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
		if balance.Info == nil {
			logger.Sugar().Errorf("fail to get wallet balance")
			continue
		}

		logger.Sugar().Infof("payment %v checking coin %v balance %v start amount %v pay amount %v",
			pay.ID, coinInfo.Name, balance.Info.Balance, pay.StartAmount, pay.Amount)

		newState := pay.State
		if balance.Info.Balance-pay.StartAmount >= pay.Amount {
			newState = orderconst.PaymentStateDone
			pay.FinishAmount = balance.Info.Balance

			myAmount := balance.Info.Balance - pay.StartAmount - pay.Amount
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
			pay.FinishAmount = balance.Info.Balance
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
			if myPayment.Info == nil {
				logger.Sugar().Errorf("fail to get good payment")
				continue
			}

			myPayment.Info.Idle = true
			myPayment.Info.OccupiedBy = ""

			_, err = grpc2.UpdateGoodPayment(ctx, &billingpb.UpdateGoodPaymentRequest{
				Info: myPayment.Info,
			})
			if err != nil {
				logger.Sugar().Errorf("fail to update good payment: %v", err)
			}

			lockKey := AccountLockKey(account.ID)
			err = redis2.Unlock(lockKey)
			if err != nil {
				logger.Sugar().Errorf("fail unlock %v: %v", lockKey, err)
			}
		}
	}
}

func checkAndTransfer(ctx context.Context, payment *billingpb.GoodPayment, coinInfo *coininfopb.CoinInfo) error { //nolint
	lockKey := AccountLockKey(payment.AccountID)
	err := redis2.TryLock(lockKey, 10*time.Minute)
	if err != nil {
		return xerrors.Errorf("fail lock account: %v", err)
	}
	defer func() {
		payment.Idle = false
		payment.OccupiedBy = ""
		_, err = grpc2.UpdateGoodPayment(ctx, &billingpb.UpdateGoodPaymentRequest{
			Info: payment,
		})
		if err != nil {
			logger.Sugar().Errorf("fail to update good payment: %v", err)
		}
		err = redis2.Unlock(lockKey)
		if err != nil {
			logger.Sugar().Errorf("fail to unlock account")
		}
	}()

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
	if err != nil {
		return xerrors.Errorf("fail get account: %v", err)
	}

	balance, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coinInfo.Name,
		Address: account.Address,
	})
	if err != nil {
		return xerrors.Errorf("fail get wallet balance: %v", err)
	}

	coinLimit := 0

	coinsetting, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
		CoinTypeID: coinInfo.ID,
	})
	if err != nil {
		return xerrors.Errorf("fail get coin setting: %v", err)
	}
	if coinsetting != nil {
		coinLimit = int(coinsetting.PaymentAccountCoinAmount)
	} else {
		platformsetting, err := grpc2.GetPlatformSetting(ctx, &billingpb.GetPlatformSettingRequest{})
		if err != nil {
			return xerrors.Errorf("fail get platform setting: %v", err)
		}
		price, err := currency.USDPrice(ctx, coinInfo.Name)
		if err != nil {
			return xerrors.Errorf("fail get price: %v", err)
		}
		coinLimit = int(platformsetting.PaymentAccountUSDAmount / price)
	}

	if int(balance.Info.Balance) > coinLimit {
		coinsetting, err := grpc2.GetCoinSettingByCoin(ctx, &billingpb.GetCoinSettingByCoinRequest{
			CoinTypeID: payment.PaymentCoinTypeID,
		})
		if err != nil || coinsetting == nil {
			return xerrors.Errorf("fail get coin setting: %v", err)
		}

		// Here we just create transaction, watcher will process it
		_, err = grpc2.CreateCoinAccountTransaction(ctx, &billingpb.CreateCoinAccountTransactionRequest{
			Info: &billingpb.CoinAccountTransaction{
				AppID:              uuid.UUID{}.String(),
				UserID:             uuid.UUID{}.String(),
				FromAddressID:      payment.AccountID,
				ToAddressID:        coinsetting.GoodIncomingAccountID,
				CoinTypeID:         coinInfo.ID,
				Amount:             balance.Info.Balance - coinInfo.ReservedAmount,
				Message:            fmt.Sprintf("payment collecting transfer of %v at %v", payment.GoodID, time.Now()),
				ChainTransactionID: uuid.New().String(),
			},
		})
		if err != nil {
			return xerrors.Errorf("fail create transaction")
		}
	}

	return nil
}

func watchPaymentAmount(ctx context.Context) {
	resp, err := grpc2.GetGoodPayments(ctx, &billingpb.GetGoodPaymentsRequest{})
	if err != nil {
		logger.Sugar().Errorf("fail get good payments: %v", err)
		return
	}

	coins := map[string]*coininfopb.CoinInfo{}

	for _, payment := range resp.Infos {
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

func AccountLockKey(accountID string) string {
	return fmt.Sprintf("%v:%v", orderconst.OrderPaymentLockKeyPrefix, accountID)
}
