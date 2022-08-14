package archivement

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/shopspring/decimal"

	ordercli "github.com/NpoolPlatform/cloud-hashing-order/pkg/client"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/const"
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"
	ordermgrpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order/order"

	goodscli "github.com/NpoolPlatform/cloud-hashing-goods/pkg/client"
	goodspb "github.com/NpoolPlatform/message/npool/cloud-hashing-goods"

	archivementcli "github.com/NpoolPlatform/archivement-middleware/pkg/client/archivement"
	detailpb "github.com/NpoolPlatform/message/npool/inspire/mgr/v1/archivement/detail"

	constant "github.com/NpoolPlatform/staker-manager/pkg/message/const"
	"github.com/NpoolPlatform/staker-manager/pkg/referral"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	scodes "go.opentelemetry.io/otel/codes"
)

func calculateArchivement(ctx context.Context, order *orderpb.Order, payment *orderpb.Payment, good *goodspb.GoodInfo) error { //nolint
	inviters, settings, err := referral.GetReferrals(ctx, order.AppID, order.UserID)
	if err != nil {
		return err
	}

	amountD := decimal.NewFromFloat(payment.Amount)
	balanceAmount, _ := decimal.NewFromString(payment.PayWithBalanceAmount) //nolint
	amountD = amountD.Add(balanceAmount)

	amount := amountD.String()
	usdAmountD := amountD.Mul(decimal.NewFromFloat(payment.CoinUSDCurrency))
	usdAmount := usdAmountD.String()
	currency := decimal.NewFromFloat(payment.CoinUSDCurrency).String()

	subPercent := uint32(0)
	var subContributor *string

	for _, inviter := range inviters {
		myInviter := inviter
		commissionD := decimal.NewFromInt(0)

		if order.OrderType == ordermgrpb.OrderType_Normal.String() ||
			order.OrderType == orderconst.OrderTypeNormal {
			sets := settings[inviter]
			for _, set := range sets {
				if set.GoodID != order.GoodID {
					continue
				}

				if set.Start > payment.CreateAt {
					continue
				}

				if set.End > 0 && set.End < payment.CreateAt {
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
		selfOrder := inviter == payment.UserID

		detailReq := &detailpb.DetailReq{
			AppID:                  &payment.AppID,
			UserID:                 &myInviter,
			DirectContributorID:    subContributor,
			GoodID:                 &order.GoodID,
			OrderID:                &order.ID,
			PaymentID:              &payment.ID,
			CoinTypeID:             &good.CoinInfoID,
			PaymentCoinTypeID:      &payment.CoinInfoID,
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
	case orderconst.OrderTypeOffline:
	case orderconst.OrderTypeNormal:
	case ordermgrpb.OrderType_Normal.String():
	case ordermgrpb.OrderType_Offline.String():
	default:
		return nil
	}

	good, err := goodscli.GetGood(ctx, order.GoodID)
	if err != nil {
		return err
	}

	payment, err := ordercli.GetOrderPayment(ctx, orderID)
	if err != nil {
		return err
	}

	ba, _ := decimal.NewFromString(payment.PayWithBalanceAmount) //nolint
	if ba.Add(decimal.NewFromFloat(payment.Amount)).Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	switch payment.State {
	case orderconst.PaymentStateDone:
	default:
		logger.Sugar().Errorw("CalculateOrderArchivement", "payment", payment.ID, "state", payment.State)
		return nil
	}

	if err := calculateArchivement(ctx, order, payment, good); err != nil {
		return err
	}

	return nil
}