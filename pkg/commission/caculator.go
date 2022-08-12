package commission

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	coininfocli "github.com/NpoolPlatform/sphinx-coininfo/pkg/client"

	constant "github.com/NpoolPlatform/staker-manager/pkg/message/const"
	"github.com/NpoolPlatform/staker-manager/pkg/referral"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	scodes "go.opentelemetry.io/otel/codes"
)

func tryUpdateCommissionLedger(
	ctx context.Context,
	appID, userID, subContributor, orderUserID, orderID, paymentID, coinTypeID string,
	amount, currency decimal.Decimal,
	createdAt uint32, oldOrder bool,
) error {
	commissionCoinID := coinTypeID

	if oldOrder {
		// For old order, always transfer to USDT TRC20
		coins, err := coininfocli.GetCoinInfos(ctx, cruder.NewFilterConds())
		if err != nil {
			return err
		}

		trc20CoinID := ""
		for _, coin := range coins {
			if coin.Name == "usdttrc20" {
				trc20CoinID = coin.ID
				break
			}
		}

		if trc20CoinID == "" {
			return fmt.Errorf("invalid trc20 coin")
		}

		const upgradeAt = uint32(1660492800) // 2022-8-15

		if createdAt < upgradeAt && coinTypeID != trc20CoinID {
			commissionCoinID = trc20CoinID
			amount = amount.Mul(currency)
		}
	}

	ioExtra := fmt.Sprintf(
		`{"PaymentID":"%v","OrderID":"%v","DirectContributorID":"%v",,"OrderUserID":"%v"}`,
		paymentID, orderID, subContributor, orderUserID)
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Commission

	amountStr := amount.String()

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &appID,
		UserID:     &userID,
		CoinTypeID: &commissionCoinID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amountStr,
		IOExtra:    &ioExtra,
	})
}

// TODO: calculate commission according to different app commission strategy
func calculateCommission(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment, oldOrder bool) error {
	inviters, settings, err := referral.GetReferrals(ctx, order.AppID, order.UserID)
	if err != nil {
		return err
	}

	percent := uint32(0)
	subPercent := uint32(0)
	subContributor := ""

	for _, user := range inviters {
		sets := settings[user]
		for _, set := range sets {
			if set.GoodID != order.GoodID {
				continue
			}

			if set.End != 0 {
				continue
			}

			if set.Start > payment.CreateAt || set.End < payment.CreateAt {
				continue
			}

			subPercent = set.Percent
			break
		}

		if percent < subPercent {
			break
		}

		if percent == subPercent {
			continue
		}

		amount := decimal.NewFromFloat(payment.Amount)
		// Also calculate balance as amount
		balanceAmount, _ := decimal.NewFromString(payment.PayWithBalanceAmount) //nolint
		amount = amount.Add(balanceAmount)

		amount = amount.Mul(decimal.NewFromInt(int64(percent - subPercent)))
		amount = amount.Div(decimal.NewFromInt(100)) //nolint

		if err := tryUpdateCommissionLedger(
			ctx, payment.AppID, user, subContributor, payment.UserID,
			order.ID, payment.ID, payment.CoinInfoID,
			amount, decimal.NewFromFloat(payment.CoinUSDCurrency),
			payment.CreateAt, oldOrder,
		); err != nil {
			return err
		}

		subPercent = percent
		subContributor = user
	}

	return nil
}

func CalculateCommission(ctx context.Context, orderID string, oldOrder bool) error {
	var err error

	_, span := otel.Tracer(constant.ServiceName).Start(ctx, "CreateGeneral")
	defer span.End()

	span.SetAttributes(attribute.String("OrderID", orderID))

	defer func() {
		if err != nil {
			span.SetStatus(scodes.Error, err.Error())
			span.RecordError(err)
		}
	}()

	order, err := ordercli.GetOrder(ctx, orderID)
	if err != nil {
		return err
	}

	payment, err := ordercli.GetOrderPayment(ctx, orderID)
	if err != nil {
		return err
	}

	if payment.Amount <= 0 {
		return nil
	}

	switch payment.State {
	case orderconst.PaymentStateDone:
	default:
		return nil
	}

	if err := calculateCommission(ctx, order, payment, oldOrder); err != nil {
		return err
	}

	return nil
}
