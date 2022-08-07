package commission

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/shopspring/decimal"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

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
	appID, userID, orderUserID, orderID, paymentID, coinTypeID string,
	amount decimal.Decimal,
) error {
	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v", "OrderUserID": "%v"}`, paymentID, orderID, orderUserID)
	amountStr := amount.String()
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Commission

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &appID,
		UserID:     &userID,
		CoinTypeID: &coinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amountStr,
		IOExtra:    &ioExtra,
	})
}

// TODO: calculate commission according to different app commission strategy
func calculateCommission(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment) error {
	inviters, settings, err := referral.GetReferrals(ctx, order.AppID, order.UserID)
	if err != nil {
		return err
	}

	percent := uint32(0)
	subPercent := uint32(0)

	for _, user := range inviters {
		sets := settings[user]
		for _, set := range sets {
			if set.Start <= payment.CreateAt && (set.End == 0 || payment.CreateAt <= set.End) {
				percent = set.Percent
				break
			}
		}

		if percent < subPercent {
			break
		}

		if percent == subPercent {
			continue
		}

		amount := decimal.NewFromFloat(payment.Amount)
		amount = amount.Mul(decimal.NewFromInt(int64(percent - subPercent)))
		amount = amount.Div(decimal.NewFromInt(100)) //nolint

		if err := tryUpdateCommissionLedger(
			ctx, payment.AppID, user, payment.UserID,
			order.ID, payment.ID, payment.CoinInfoID, amount,
		); err != nil {
			return err
		}

		subPercent = percent
	}

	return nil
}

func CalculateCommission(ctx context.Context, orderID string) error {
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

	switch payment.State {
	case orderconst.PaymentStateDone:
	default:
		return fmt.Errorf("invalid payment state")
	}

	if err := calculateCommission(ctx, order, payment); err != nil {
		return err
	}

	return nil
}
