package executor

import (
	"context"
	"fmt"

	logger "github.com/NpoolPlatform/go-service-framework/pkg/logger"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	ledgerstatementmwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger/statement"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	paymentaccountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	ledgertypes "github.com/NpoolPlatform/message/npool/basetypes/ledger/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	ledgerstatementmwpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/ledger/statement"
	paymentmwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/payment"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/payment/obselete/transfer/bookkeeping/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type paymentHandler struct {
	*paymentmwpb.Payment
	persistent           chan interface{}
	done                 chan interface{}
	paymentTransferCoins map[string]*coinmwpb.Coin
	paymentAccounts      map[string]*paymentaccountmwpb.Account
	statements           []*ledgerstatementmwpb.StatementReq
	paymentTransfers     []*paymentmwpb.PaymentTransferReq
}

func (h *paymentHandler) checkPaymentStatement(ctx context.Context) (bool, error) {
	return ledgerstatementmwcli.ExistStatementConds(ctx, &ledgerstatementmwpb.Conds{
		AppID:     &basetypes.StringVal{Op: cruder.EQ, Value: h.AppID},
		UserID:    &basetypes.StringVal{Op: cruder.EQ, Value: h.UserID},
		IOSubType: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(ledgertypes.IOSubType_ObseletePayment)},
		IOExtra:   &basetypes.StringVal{Op: cruder.LIKE, Value: h.EntID},
	})
}

func (h *paymentHandler) getPaymentCoins(ctx context.Context) (err error) {
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
	return nil
}

func (h *paymentHandler) getPaymentAccounts(ctx context.Context) (err error) {
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

func (h *paymentHandler) constructStatement(ctx context.Context, transfer *paymentmwpb.PaymentTransferInfo) error {
	coin, ok := h.paymentTransferCoins[transfer.CoinTypeID]
	if !ok {
		return wlog.Errorf("invalid coin")
	}
	account, ok := h.paymentAccounts[transfer.AccountID]
	if !ok {
		return wlog.Errorf("invalid paymentaccount")
	}
	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    coin.Name,
		Address: account.Address,
	})
	if err != nil {
		return wlog.WrapError(err)
	}
	if balance == nil {
		return wlog.Errorf("invalid balance")
	}
	bal, err := decimal.NewFromString(balance.BalanceStr)
	if err != nil {
		return err
	}
	startAmount, err := decimal.NewFromString(transfer.StartAmount)
	if err != nil {
		return err
	}
	h.paymentTransfers = append(h.paymentTransfers, &paymentmwpb.PaymentTransferReq{
		EntID:        &transfer.EntID,
		FinishAmount: &balance.BalanceStr,
	})
	if bal.Cmp(startAmount) <= 0 {
		return nil
	}
	h.statements = append(h.statements, &ledgerstatementmwpb.StatementReq{
		EntID:      func() *string { s := uuid.NewString(); return &s }(),
		AppID:      &h.AppID,
		UserID:     &h.UserID,
		CoinTypeID: &transfer.CoinTypeID,
		IOType:     func() *ledgertypes.IOType { e := ledgertypes.IOType_Incoming; return &e }(),
		IOSubType:  func() *ledgertypes.IOSubType { e := ledgertypes.IOSubType_ObseletePayment; return &e }(),
		Amount:     func() *string { s := bal.Sub(startAmount).String(); return &s }(),
		IOExtra: func() *string {
			s := fmt.Sprintf(
				`{"OrderID":"%v", "PaymentID": "%v", "Reason": "ObseletePayment"}`,
				h.OrderID,
				h.EntID,
			)
			return &s
		}(),
	})
	return nil
}

func (h *paymentHandler) constructStatements(ctx context.Context) error {
	for _, transfer := range h.PaymentTransfers {
		if err := h.constructStatement(ctx, transfer); err != nil {
			return wlog.WrapError(err)
		}
	}
	return nil
}

//nolint:gocritic
func (h *paymentHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Payment", h,
			"PaymentTransfer", h.paymentTransfers,
			"Error", *err,
		)
	}
	persistentPayment := &types.PersistentPayment{
		Payment:          h.Payment,
		Statements:       h.statements,
		PaymentTransfers: h.paymentTransfers,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentPayment, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentPayment, h.done)
}

//nolint:gocritic
func (h *paymentHandler) exec(ctx context.Context) error {
	var err error
	var exist bool
	defer h.final(ctx, &err)

	if exist, err = h.checkPaymentStatement(ctx); err != nil || exist {
		return err
	}
	if err = h.getPaymentCoins(ctx); err != nil {
		return err
	}
	if err = h.getPaymentAccounts(ctx); err != nil {
		return err
	}
	if err = h.constructStatements(ctx); err != nil {
		return wlog.WrapError(err)
	}

	return nil
}
