package benefit

import (
	"context"
	"fmt"
	"time"

	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	// TODO: move mining profit to middleware TX
	profitdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/mining/profit/detail"
	profitgeneralpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/mining/profit/general"
	profitunsoldpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/mining/profit/unsold"
	profitdetailcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/detail"
	profitgeneralcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/general"
	profitunsoldcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/unsold"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"

	"github.com/google/uuid"
)

// TODO: support multiple coin type profit of one good

type gp struct {
	goodID          string
	goodName        string
	profitGeneralID string

	totalUnits      uint32 // from stock
	inService       uint32 // from stock for verification
	totalOrderUnits uint32 // same as serviceUnits but used before actual transferring
	serviceUnits    uint32 // sold units

	benefitAddress       string
	benefitAccountID     string
	benefitIntervalHours uint32
	dailyProfit          decimal.Decimal
	initialKept          decimal.Decimal

	userOnlineAccountID      string
	platformOfflineAccountID string

	coinName           string
	coinTypeID         string
	coinReservedAmount decimal.Decimal

	transferredToUser     decimal.Decimal
	transferredToPlatform decimal.Decimal
}

func (g *gp) profitExist(ctx context.Context, timestamp time.Time) (bool, error) {
	detail, err := profitdetailcli.GetDetailOnly(ctx, &profitdetailpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.goodID,
		},
		BenefitDate: &commonpb.Uint32Val{
			Op:    cruder.EQ,
			Value: uint32(timestamp.Unix()),
		},
	})
	if err != nil {
		return false, err
	}

	return detail != nil, nil
}

func (g *gp) profitBalance(ctx context.Context) (decimal.Decimal, error) {
	general, err := profitgeneralcli.GetGeneralOnly(ctx, &profitgeneralpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.goodID,
		},
	})
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if general == nil {
		general, err = profitgeneralcli.CreateGeneral(ctx, &profitgeneralpb.GeneralReq{
			GoodID:     &g.goodID,
			CoinTypeID: &g.coinTypeID,
		})
		if err != nil {
			return decimal.NewFromInt(0), err
		}

		g.profitGeneralID = general.ID

		return decimal.NewFromInt(0), nil
	}

	g.profitGeneralID = general.ID

	amount, err := decimal.NewFromString(general.Amount)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	toPlatform, err := decimal.NewFromString(general.TransferredToPlatform)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	toUser, err := decimal.NewFromString(general.TransferredToUser)
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	remain := amount.Sub(toPlatform).Sub(toUser)
	if remain.Cmp(decimal.NewFromInt(0)) < 0 {
		return decimal.NewFromInt(0), fmt.Errorf("invalid profit general")
	}

	return remain, nil
}

func (g *gp) addDailyProfit(ctx context.Context, timestamp time.Time) error {
	if g.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid profit amount")
	}

	if g.totalUnits <= 0 {
		return fmt.Errorf("invalid stock units")
	}

	amount := g.dailyProfit.String()
	toUserD := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(g.serviceUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits)))
	toUser := toUserD.String()
	toPlatformD := g.dailyProfit.
		Sub(toUserD)
	toPlatform := toPlatformD.String()

	tsUnix := uint32(timestamp.Unix())

	transferredToPlatform := g.transferredToPlatform.String()
	transferredToUser := g.transferredToUser.String()

	logger.Sugar().Infow("addDailyProfit", "goodID", g.goodID, "goodName", g.goodName, "dailyProfit", g.dailyProfit,
		"userAmount", toUser, "platformAmount", toPlatform, "transferredToUser",
		transferredToUser, "transferredToPlatform", transferredToPlatform, "initialKept", g.initialKept)

	if toUserD.Cmp(decimal.NewFromInt(0)) > 0 {
		_, err := profitdetailcli.CreateDetail(ctx, &profitdetailpb.DetailReq{
			GoodID:      &g.goodID,
			CoinTypeID:  &g.coinTypeID,
			Amount:      &amount,
			BenefitDate: &tsUnix,
		})
		if err != nil {
			return err
		}
	}

	if toPlatformD.Cmp(decimal.NewFromInt(0)) > 0 {
		_, err := profitgeneralcli.AddGeneral(ctx, &profitgeneralpb.GeneralReq{
			ID:                    &g.profitGeneralID,
			Amount:                &amount,
			ToPlatform:            &toPlatform,
			ToUser:                &toUser,
			TransferredToPlatform: &transferredToPlatform,
			TransferredToUser:     &transferredToUser,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *gp) benefitBalance(ctx context.Context) (decimal.Decimal, error) {
	benefit, err := billingcli.GetGoodBenefit(ctx, g.goodID)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if benefit == nil {
		return decimal.NewFromInt(0), fmt.Errorf("invalid good benefit setting")
	}

	g.benefitAccountID = benefit.BenefitAccountID
	g.benefitIntervalHours = benefit.BenefitIntervalHours

	account, err := billingcli.GetAccount(ctx, g.benefitAccountID)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if account == nil {
		return decimal.NewFromInt(0), fmt.Errorf("invalid benefit account")
	}

	g.benefitAddress = account.Address

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    g.coinName,
		Address: g.benefitAddress,
	})
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	return decimal.NewFromString(balance.BalanceStr)
}

func (g *gp) processDailyProfit(ctx context.Context, timestamp time.Time) error {
	exist, err := g.profitExist(ctx, timestamp)
	if err != nil {
		return err
	}
	if exist {
		return fmt.Errorf("daily profit exist")
	}

	remain, err := g.profitBalance(ctx)
	if err != nil {
		return err
	}

	balance, err := g.benefitBalance(ctx)
	if err != nil {
		return err
	}

	logger.Sugar().Infow("processDailyProfit", "goodID", g.goodID, "goodName", g.goodName, "benefitAddress",
		g.benefitAddress, "remain", remain, "balance", balance, "reserveAmount", g.coinReservedAmount)

	if balance.Cmp(remain) <= 0 {
		return nil
	}

	g.dailyProfit = balance.Sub(remain)

	if remain.Cmp(g.coinReservedAmount) < 0 {
		g.initialKept = g.coinReservedAmount
	}

	return nil
}

func (g *gp) stock(ctx context.Context) error {
	goodInfo, err := goodscli.GetGood(ctx, g.goodID)
	if err != nil {
		return err
	}

	if goodInfo == nil {
		return fmt.Errorf("invalid good")
	}

	g.totalUnits = goodInfo.GetGoodTotal()
	g.inService = goodInfo.GetGoodInService()

	return nil
}

func (g *gp) processOrder(ctx context.Context, order *orderpb.Order, timestamp time.Time) error {
	amount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(order.Units))).
		Div(decimal.NewFromInt(int64(g.totalUnits))).
		String()
	ioExtra := fmt.Sprintf(`{"GoodID": "%v", "BenefitDate": "%v", "OrderID": "%v"}`,
		g.goodID, timestamp, order.ID)

	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_MiningBenefit

	return ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &g.coinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
}

func (g *gp) processUnsold(ctx context.Context, timestamp time.Time) error {
	unsold, err := profitunsoldcli.GetUnsoldOnly(ctx, &profitunsoldpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.goodID,
		},
		BenefitDate: &commonpb.Uint32Val{
			Op:    cruder.EQ,
			Value: uint32(timestamp.Unix()),
		},
	})
	if err != nil {
		return err
	}
	if unsold != nil {
		return fmt.Errorf("profit unsold exist")
	}

	amount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(g.totalUnits - g.serviceUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits))).
		String()
	tsUnix := uint32(timestamp.Unix())

	_, err = profitunsoldcli.CreateUnsold(ctx, &profitunsoldpb.UnsoldReq{
		GoodID:      &g.goodID,
		CoinTypeID:  &g.coinTypeID,
		Amount:      &amount,
		BenefitDate: &tsUnix,
	})

	return err
}

func (g *gp) transfer(ctx context.Context, timestamp time.Time) error {
	if g.dailyProfit.Cmp(g.initialKept) <= 0 {
		return nil
	}

	if g.totalUnits <= 0 {
		return fmt.Errorf("invalid stock units")
	}

	userAmount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(g.serviceUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits)))
	platformAmount := g.dailyProfit.
		Sub(g.initialKept).
		Sub(userAmount)

	logger.Sugar().Infow("transfer", "goodID", g.goodID, "goodName", g.goodName, "dailyProfit", g.dailyProfit,
		"userAmount", userAmount, "platformAmount", platformAmount, "initialKept", g.initialKept)

	if userAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		_, err := billingcli.CreateTransaction(ctx, &billingpb.CoinAccountTransaction{
			AppID:         uuid.UUID{}.String(),
			UserID:        uuid.UUID{}.String(),
			GoodID:        g.goodID,
			CoinTypeID:    g.coinTypeID,
			FromAddressID: g.benefitAccountID,
			ToAddressID:   g.userOnlineAccountID,
			Amount:        userAmount.InexactFloat64(),
			Message:       fmt.Sprintf(`{"GoodID": "%v", "Amount": "%v", "BenefitDate": "%v"}`, g.goodID, userAmount, timestamp),
			CreatedFor:    billingconst.TransactionForUserBenefit,
		})
		if err != nil {
			return err
		}
		g.transferredToUser = userAmount
	}

	if platformAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	_, err := billingcli.CreateTransaction(ctx, &billingpb.CoinAccountTransaction{
		AppID:         uuid.UUID{}.String(),
		UserID:        uuid.UUID{}.String(),
		GoodID:        g.goodID,
		CoinTypeID:    g.coinTypeID,
		FromAddressID: g.benefitAccountID,
		ToAddressID:   g.platformOfflineAccountID,
		Amount:        platformAmount.InexactFloat64(),
		Message:       fmt.Sprintf(`{"GoodID": "%v", "Amount": "%v", "BenefitDate": "%v"}`, g.goodID, userAmount, timestamp),
		CreatedFor:    billingconst.TransactionForPlatformBenefit,
	})
	if err != nil {
		return err
	}

	g.transferredToPlatform = platformAmount

	return nil
}
