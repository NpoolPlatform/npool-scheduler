package paymentwatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"

	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	grpc2 "github.com/NpoolPlatform/cloud-hashing-staker/pkg/grpc"

	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"
	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
)

func watchPaymentState(ctx context.Context) { //nolint
	payments, err := grpc2.GetPaymentsByState(ctx, &orderpb.GetPaymentsByStateRequest{
		State: orderconst.PaymentStateWait,
	})
	if err != nil {
		logger.Sugar().Errorf("fail to get wait payments: %v", err)
		return
	}

	for _, pay := range payments.Infos {
		coinInfo, err := grpc2.GetCoinInfo(ctx, &coininfopb.GetCoinInfoRequest{
			ID: pay.CoinInfoID,
		})
		if err != nil {
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

		balance, err := grpc2.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    coinInfo.Info.Name,
			Address: account.Info.Address,
		})
		if err != nil {
			logger.Sugar().Errorf("fail to get wallet balance: %v", err)
			continue
		}

		logger.Sugar().Infof("payment %v checking coin %v balance %v start amount %v pay amount %v",
			pay.ID, coinInfo.Info.Name, balance.Info.Balance, pay.StartAmount, pay.Amount)

		newState := pay.State
		if balance.Info.Balance-pay.StartAmount > pay.Amount {
			newState = orderconst.PaymentStateDone
		}
		if pay.CreateAt+orderconst.TimeoutSeconds < uint32(time.Now().Unix()) {
			newState = orderconst.PaymentStateTimeout
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
				AccountID: account.Info.ID,
			})
			if err != nil {
				logger.Sugar().Errorf("fail to get good payment: %v", err)
				continue
			}

			// TODO: add incoming transaction

			myPayment.Info.Idle = true
			_, err = grpc2.UpdateGoodPayment(ctx, &billingpb.UpdateGoodPaymentRequest{
				Info: myPayment.Info,
			})
			if err != nil {
				logger.Sugar().Errorf("fail to update good payment: %v", err)
			}

			lockKey := AccountLockKey(account.Info.ID)
			err = redis2.Unlock(lockKey)
			if err != nil {
				logger.Sugar().Errorf("fail unlock %v: %v", lockKey, err)
			}
		}
	}
}

func Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)

	for { //nolint
		select {
		case <-ticker.C:
			watchPaymentState(ctx)
		}
	}
}

func AccountLockKey(accountID string) string {
	return fmt.Sprintf("%v:%v", orderconst.OrderPaymentLockKeyPrefix, accountID)
}
