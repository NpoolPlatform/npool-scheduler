package archivement

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/shopspring/decimal"

	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"

	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order"
	ordercli "github.com/NpoolPlatform/order-middleware/pkg/client/order"

	paymentmgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/payment"

	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodspb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"

	archivementcli "github.com/NpoolPlatform/archivement-middleware/pkg/client/archivement"
	detailpb "github.com/NpoolPlatform/message/npool/inspire/mgr/v1/archivement/detail"

	constant "github.com/NpoolPlatform/staker-manager/pkg/message/const"
	"github.com/NpoolPlatform/staker-manager/pkg/referral"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	scodes "go.opentelemetry.io/otel/codes"
)

func calculateArchivement(ctx context.Context, order *orderpb.Order, good *goodspb.Good) error { //nolint
	inviters, settings, err := referral.GetReferrals(ctx, order.AppID, order.UserID)
	if err != nil {
		return err
	}

	amountD, err := decimal.NewFromString(order.PaymentAmount)
	if err != nil {
		return err
	}
	balanceAmount, err := decimal.NewFromString(order.PayWithBalanceAmount) //nolint
	if err != nil {
		return err
	}
	amountD = amountD.Add(balanceAmount)

	amount := amountD.String()

	coinUSDCurrency, err := decimal.NewFromString(order.PaymentCoinUSDCurrency)
	if err != nil {
		return err
	}

	usdAmountD := amountD.Mul(coinUSDCurrency)
	usdAmount := usdAmountD.String()
	currency := coinUSDCurrency.String()

	subPercent := uint32(0)
	var subContributor *string

	for _, inviter := range inviters {
		myInviter := inviter
		commissionD := decimal.NewFromInt(0)

		if order.OrderType == ordermgrpb.OrderType_Normal {
			sets := settings[inviter]
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

				if subPercent < set.Percent {
					commissionD = commissionD.
						Add(usdAmountD.Mul(
							decimal.NewFromInt(int64(set.Percent - subPercent))).
							Div(decimal.NewFromInt(100))) //nolint
				}
				subPercent = set.Percent
				break
			}
		}

		commission := commissionD.String()
		selfOrder := inviter == order.UserID

		detailReq := &detailpb.DetailReq{
			AppID:                  &order.AppID,
			UserID:                 &myInviter,
			DirectContributorID:    subContributor,
			GoodID:                 &order.GoodID,
			OrderID:                &order.ID,
			PaymentID:              &order.PaymentID,
			CoinTypeID:             &good.CoinTypeID,
			PaymentCoinTypeID:      &order.PaymentCoinTypeID,
			PaymentCoinUSDCurrency: &currency,
			Units:                  &order.Units,
			Amount:                 &amount,
			Commission:             &commission,
			USDAmount:              &usdAmount,
			SelfOrder:              &selfOrder,
		}
		if err := archivementcli.BookKeeping(ctx, detailReq); err != nil {
			return err
		}

		subContributor = &myInviter
	}

	return nil
}

func CalculateArchivement(ctx context.Context, orderID string) error {
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
	case ordermgrpb.OrderType_Offline:
	case ordermgrpb.OrderType_Normal:
	default:
		return nil
	}

	good, err := goodscli.GetGood(ctx, order.GoodID)
	if err != nil {
		return err
	}

	payment, err := ordercli.GetOrder(ctx, orderID)
	if err != nil {
		return err
	}

	ba, _ := decimal.NewFromString(payment.PayWithBalanceAmount) //nolint
	amount, _ := decimal.NewFromString(payment.PaymentAmount)    //nolint
	if ba.Add(amount).Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	switch payment.PaymentState {
	case paymentmgrpb.PaymentState_Done:
	default:
		logger.Sugar().Errorw("CalculateOrderArchivement", "payment", payment.ID, "state", payment.PaymentState)
		return nil
	}

	if err := calculateArchivement(ctx, order, good); err != nil {
		return err
	}

	return nil
}
