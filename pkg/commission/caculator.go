package commission

import (
	"context"
	"fmt"

	paymentmgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/payment"

	"github.com/shopspring/decimal"

	ordercli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"

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
		`{"PaymentID":"%v","OrderID":"%v","DirectContributorID":"%v","OrderUserID":"%v"}`,
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
// nolint
func calculateCommission(ctx context.Context, order *orderpb.Order, oldOrder bool) error {
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

			if set.Start > order.CreatedAt {
				continue
			}

			if set.End > 0 && set.End < order.CreatedAt {
				continue
			}

			percent = set.Percent
			break
		}

		if percent < subPercent {
			break
		}
		if percent == subPercent {
			subPercent = percent
			subContributor = user
			continue
		}

		amount, _ := decimal.NewFromString(order.PaymentAmount)
		// Also calculate balance as amount
		balanceAmount, _ := decimal.NewFromString(order.PayWithBalanceAmount) //nolint
		amount = amount.Add(balanceAmount)

		amount = amount.Mul(decimal.NewFromInt(int64(percent - subPercent)))
		amount = amount.Div(decimal.NewFromInt(100)) //nolint

		paymentCoinUSDCurrency, _ := decimal.NewFromString(order.PaymentCoinUSDCurrency)

		if err := tryUpdateCommissionLedger(
			ctx, order.AppID, user, subContributor, order.UserID,
			order.ID, order.PaymentID, order.PaymentCoinTypeID,
			amount, paymentCoinUSDCurrency,
			order.CreatedAt, oldOrder,
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

	switch order.OrderType {
	case ordermgrpb.OrderType_Normal:
	default:
		return nil
	}

	paymentAmount, _ := decimal.NewFromString(order.PaymentAmount) //nolint

	ba, _ := decimal.NewFromString(order.PayWithBalanceAmount) //nolint
	if ba.Add(paymentAmount).Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	switch order.PaymentState {
	case paymentmgrpb.PaymentState_Done:
	default:
		return nil
	}

	if err := calculateCommission(ctx, order, oldOrder); err != nil {
		return err
	}

	return nil
}
