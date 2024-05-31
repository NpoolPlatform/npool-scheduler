package executor

import (
	"context"
	"fmt"

	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	feeordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/fee"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/fee/payment/bookkeeping/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type orderHandler struct {
	*feeordermwpb.FeeOrder
	done                 chan interface{}
	persistent           chan interface{}
	notif                chan interface{}
	paymentTransfers     []*types.XPaymentTransfer
	paymentTransferCoins map[string]*coinmwpb.Coin
	paymentAccounts      map[string]*paymentaccountmwpb.Account
}

func (h *orderHandler) getPaymentCoins(ctx context.Context) (err error) {
	h.paymentTransferCoins, err = schedcommon.GetCoins(ctx, func() (coinTypeIDs []string) {
		for _, paymentTransfer := range h.PaymentTransfers {
			coinTypeIDs = append(coinTypeIDs, paymentTransfer.CoinTypeID)
		}
		return
	}())
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, paymentTransfer := range h.PaymentTransfers {
		if _, ok := h.paymentTransferCoins[paymentTransfer.CoinTypeID]; !ok {
			return wlog.Errorf("invalid paymenttransfercoin")
		}
	}
	for _, paymentCoin := range h.paymentTransferCoins {
		if !paymentCoin.ForPay {
			return wlog.Errorf("invalid paymenttransfercoin")
		}
	}
	return nil
}

func (h *orderHandler) getPaymentAccounts(ctx context.Context) (err error) {
	h.paymentAccounts, err = schedcommon.GetPaymentAccounts(ctx, func() (accountIDs []string) {
		for _, paymentTransfer := range h.PaymentTransfers {
			accountIDs = append(accountIDs, paymentTransfer.AccountID)
		}
		return
	}())
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, paymentTransfer := range h.PaymentTransfers {
		if _, ok := h.paymentAccounts[paymentTransfer.AccountID]; !ok {
			return wlog.Errorf("invalid paymentaccount")
		}
	}
	return nil
}

func (h *orderHandler) updatePaymentTransfers(ctx context.Context) error {
	for _, paymentTransfer := range h.paymentTransfers {
		paymentCoin, ok := h.paymentTransferCoins[paymentTransfer.CoinTypeID]
		if !ok {
			return wlog.Errorf("invalid paymentcoin")
		}
		paymentAccount, ok := h.paymentAccounts[paymentTransfer.AccountID]
		if !ok {
			return wlog.Errorf("invalid paymentaccount")
		}

		balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
			Name:    paymentCoin.Name,
			Address: paymentAccount.Address,
		})
		if err != nil {
			return wlog.WrapError(err)
		}
		if balance == nil {
			return wlog.Errorf("invalid balance")
		}

		bal, err := decimal.NewFromString(balance.BalanceStr)
		if err != nil {
			return wlog.WrapError(err)
		}
		paymentTransfer.IncomingAmount = func() *string {
			amount := bal.Sub(paymentTransfer.StartAmount)
			if amount.Cmp(decimal.NewFromInt(0)) <= 0 {
				return nil
			}
			s := amount.String()
			return &s
		}()
		// If we could be here, we should have enough balance
		if paymentTransfer.IncomingAmount != nil {
			paymentTransfer.IncomingExtra = fmt.Sprintf(
				`{"PaymentID": "%v","OrderID":"%v","PaymentState":"%v","GoodID":"%v","AppGoodID":"%v"}`,
				h.PaymentID,
				h.EntID,
				h.PaymentState,
				h.GoodID,
				h.AppGoodID,
			)
			paymentTransfer.OutcomingExtra = fmt.Sprintf(
				`{"PaymentID":"%v","OrderID": "%v","FromTransfer":true,"GoodID":"%v","AppGoodID":"%v","PaymentType":"%v"}`,
				h.PaymentID,
				h.EntID,
				h.GoodID,
				h.AppGoodID,
				h.PaymentType,
			)
		}
		paymentTransfer.FinishAmount = balance.BalanceStr
	}
	return nil
}

//nolint:gocritic
func (h *orderHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"FeeOrder", h.FeeOrder,
			"PaymentTransfers", h.paymentTransfers,
			"Error", *err,
		)
	}

	persistentOrder := &types.PersistentOrder{
		FeeOrder:          h.FeeOrder,
		XPaymentTransfers: h.paymentTransfers,
		Error:             *err,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentOrder, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentOrder, h.done)
}

//nolint:gocritic
func (h *orderHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if err = h.getPaymentCoins(ctx); err != nil {
		return err
	}
	if err = h.getPaymentAccounts(ctx); err != nil {
		return err
	}
	if err = h.updatePaymentTransfers(ctx); err != nil {
		return err
	}
	return nil
}
