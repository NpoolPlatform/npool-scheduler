package commission

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/shopspring/decimal"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	ledgerdetailcli "github.com/NpoolPlatform/ledger-manager/pkg/client/detail"
	ledgergeneralcli "github.com/NpoolPlatform/ledger-manager/pkg/client/general"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"
	ledgergeneralpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/general"

	commonpb "github.com/NpoolPlatform/message/npool"

	constant "github.com/NpoolPlatform/staker-manager/pkg/message/const"
	"github.com/NpoolPlatform/staker-manager/pkg/referral"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

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
	spendable := amount.String()
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Commission

	detail, err := ledgerdetailcli.GetDetailOnly(ctx, &ledgerdetailpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: appID,
		},
		UserID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: userID,
		},
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: coinTypeID,
		},
		IOType: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(ioType),
		},
		IOSubType: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(ioSubType),
		},
		IOExtra: &commonpb.StringVal{
			Op:    cruder.LIKE,
			Value: orderID,
		},
	})
	if err != nil {
		return err
	}
	if detail != nil {
		return fmt.Errorf("commission already exist")
	}

	_, err = ledgerdetailcli.CreateDetail(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &appID,
		UserID:     &userID,
		CoinTypeID: &coinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amountStr,
		IOExtra:    &ioExtra,
	})
	if err != nil {
		return err
	}

	_, err = ledgergeneralcli.AddGeneral(ctx, &ledgergeneralpb.GeneralReq{
		Incoming:  &amountStr,
		Spendable: &spendable,
	})

	return err
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

		if percent <= subPercent {
			logger.Sugar().Errorw("calculateCommission", "user", user, "percent", percent, "subPercent", subPercent, "users", inviters)
			break
		}

		amount := decimal.NewFromFloat(payment.Amount)
		amount = amount.Mul(decimal.NewFromInt(int64(percent - subPercent)))
		amount = amount.Div(decimal.NewFromInt(100)) //nolint

		if err := tryUpdateCommissionLedger(ctx, payment.AppID, user, payment.UserID,
			order.ID, payment.ID, payment.CoinInfoID, amount); err != nil {
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
		logger.Sugar().Errorw("CalculateOrderCommission", "payment", payment.ID, "state", payment.State)
		return fmt.Errorf("invalid payment state")
	}

	if err := calculateCommission(ctx, order, payment); err != nil {
		return err
	}

	return nil
}
