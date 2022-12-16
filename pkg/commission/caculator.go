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

	constant "github.com/NpoolPlatform/staker-manager/pkg/message/const"
	"github.com/NpoolPlatform/staker-manager/pkg/referral"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	scodes "go.opentelemetry.io/otel/codes"
)

func tryUpdateCommissionLedger(
	ctx context.Context,
	appID, userID, subContributor, orderUserID, orderID, paymentID, coinTypeID string,
	amount decimal.Decimal,
	oldOrder bool,
) error {
	commissionCoinID := coinTypeID

	if oldOrder {
		return fmt.Errorf("invalid old order")
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

		if err := tryUpdateCommissionLedger(
			ctx, order.AppID, user, subContributor, order.UserID,
			order.ID, order.PaymentID, order.PaymentCoinTypeID,
			amount, oldOrder,
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
