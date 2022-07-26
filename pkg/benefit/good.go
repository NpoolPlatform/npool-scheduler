package benefit

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	profitdetailpb "github.com/NpoolPlatform/message/npool/miningmgr/profit/detail"
	profitgeneralpb "github.com/NpoolPlatform/message/npool/miningmgr/profit/general"
	profitunsoldpb "github.com/NpoolPlatform/message/npool/miningmgr/profit/unsold"
	profitdetailcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/detail"
	profitgeneralcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/general"
	profitunsoldcli "github.com/NpoolPlatform/mining-manager/pkg/client/profit/unsold"

	ledgerdetailcli "github.com/NpoolPlatform/ledger-manager/pkg/client/detail"
	ledgergeneralcli "github.com/NpoolPlatform/ledger-manager/pkg/client/general"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledgermgr/detail"
	ledgergeneralpb "github.com/NpoolPlatform/message/npool/ledgermgr/general"

	stockcli "github.com/NpoolPlatform/stock-manager/pkg/client"
	stockconst "github.com/NpoolPlatform/stock-manager/pkg/const"

	commonpb "github.com/NpoolPlatform/message/npool"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"

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
	totalOrderUnits uint32 // from sold order
	serviceUnits    uint32 // unsold + sold waiting

	benefitAddress       string
	benefitAccountID     string
	benefitIntervalHours uint32

	dailyProfit decimal.Decimal
	initialKept decimal.Decimal

	userOnlineAccountID      string
	platformOfflineAccountID string

	coinName           string
	coinTypeID         string
	coinReservedAmount decimal.Decimal
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
	toPlatform, err := decimal.NewFromString(general.ToPlatform)
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	toUser, err := decimal.NewFromString(general.ToUser)
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
		Sub(g.initialKept).
		Mul(decimal.NewFromInt(int64(g.totalOrderUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits)))
	toUser := toUserD.String()
	toPlatform := g.dailyProfit.
		Sub(g.initialKept).
		Sub(toUserD).String()

	tsUnix := uint32(timestamp.Unix())

	_, err := profitdetailcli.CreateDetail(ctx, &profitdetailpb.DetailReq{
		GoodID:      &g.goodID,
		CoinTypeID:  &g.coinTypeID,
		Amount:      &amount,
		BenefitDate: &tsUnix,
	})
	if err != nil {
		return err
	}

	_, err = profitgeneralcli.AddGeneral(ctx, &profitgeneralpb.GeneralReq{
		ID:         &g.profitGeneralID,
		Amount:     &amount,
		ToPlatform: &toPlatform,
		ToUser:     &toUser,
	})

	return err
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
		g.benefitAddress, "remain", remain, "balance", balance)

	if balance.Cmp(remain) <= 0 {
		return nil
	}

	if balance.Cmp(g.coinReservedAmount) <= 0 {
		return nil
	}

	if remain.Cmp(g.coinReservedAmount) <= 0 {
		g.initialKept = g.coinReservedAmount
	}

	g.dailyProfit = balance.Sub(remain)

	return nil
}

func (g *gp) stock(ctx context.Context) error {
	stock, err := stockcli.GetStockOnly(ctx, cruder.NewFilterConds().
		WithCond(stockconst.StockFieldGoodID, cruder.EQ, structpb.NewStringValue(g.goodID)))
	if err != nil {
		return err
	}
	if stock == nil {
		return fmt.Errorf("invalid good stock")
	}

	g.totalUnits = stock.Total
	g.inService = stock.InService

	return nil
}

func (g *gp) processOrder(ctx context.Context, order *orderpb.Order, timestamp time.Time) error {
	logger.Sugar().Infow("processOrder", "timestamp", timestamp, "goodID", g.goodID, "goodName", g.goodName, "profit",
		g.dailyProfit, "totalUnits", g.totalUnits, "order", order.ID, "orderUnits", order.Units)

	amount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(order.Units))).
		Div(decimal.NewFromInt(int64(g.totalUnits))).
		String()
	ioExtra := fmt.Sprintf(`{"GoodID": "%v", "BenefitDate": "%v", "OrderID": "%v"}`,
		g.goodID, timestamp, order.ID)

	detail, err := ledgerdetailcli.GetDetailOnly(ctx, &ledgerdetailpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.AppID,
		},
		UserID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.UserID,
		},
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.coinTypeID,
		},
		IOType: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(ledgerdetailpb.IOType_Incoming),
		},
		IOSubType: &commonpb.Int32Val{
			Op:    cruder.EQ,
			Value: int32(ledgerdetailpb.IOSubType_MiningBenefit),
		},
		IOExtra: &commonpb.StringVal{
			Op:    cruder.LIKE,
			Value: ioExtra,
		},
	})
	if err != nil {
		return err
	}
	if detail != nil {
		return fmt.Errorf("ledger detail exist")
	}

	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_MiningBenefit

	_, err = ledgerdetailcli.CreateDetail(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &g.coinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	})
	if err != nil {
		return err
	}

	general, err := ledgergeneralcli.GetGeneralOnly(ctx, &ledgergeneralpb.Conds{
		AppID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.AppID,
		},
		UserID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: order.UserID,
		},
		CoinTypeID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.coinTypeID,
		},
	})
	if err != nil {
		return err
	}
	if general == nil {
		general, err = ledgergeneralcli.CreateGeneral(ctx, &ledgergeneralpb.GeneralReq{
			AppID:      &order.AppID,
			UserID:     &order.UserID,
			CoinTypeID: &g.coinTypeID,
		})
		if err != nil {
			return err
		}
	}

	_, err = ledgergeneralcli.AddGeneral(ctx, &ledgergeneralpb.GeneralReq{
		ID:        &general.ID,
		Incoming:  &amount,
		Spendable: &amount,
	})

	return err
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
	userAmount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(g.serviceUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits)))
	platformAmount := g.dailyProfit.
		Sub(g.initialKept).
		Sub(userAmount)

	logger.Sugar().Infow("transfer", "goodID", g.goodID, "goodName", g.goodName,
		"userAmount", userAmount, "platformAmount", platformAmount,
		"initialKept", g.initialKept)

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

	if platformAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	_, err = billingcli.CreateTransaction(ctx, &billingpb.CoinAccountTransaction{
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

	return err
}
