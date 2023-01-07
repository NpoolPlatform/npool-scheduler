package benefit

import (
	"context"
	"fmt"

	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	gbmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/goodbenefit"
	gbmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/goodbenefit"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	txmgrpb "github.com/NpoolPlatform/message/npool/chain/mgr/v1/tx"

	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	miningdetailcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/mining/detail"
	mininggeneralcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/mining/general"
	miningunsoldcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/mining/unsold"
	mininggeneralpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/mining/general"
	miningdetailpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/mining/detail"
	miningunsoldpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/mining/unsold"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	commonpb "github.com/NpoolPlatform/message/npool"

	"github.com/shopspring/decimal"
)

// TODO: support multiple coin type profit of one good

type gp struct {
	goodID          string
	goodName        string
	miningGeneralID string

	totalUnits      uint32 // from stock
	inService       uint32 // from stock for verification
	totalOrderUnits uint32 // same as serviceUnits but used before actual transferring
	serviceUnits    uint32 // sold units

	benefitAddress       string
	benefitAccountID     string
	benefitIntervalHours uint32
	dailyProfit          decimal.Decimal

	userOnlineAccountID      string
	platformOfflineAccountID string

	coinName           string
	coinTypeID         string
	coinReservedAmount decimal.Decimal
}

func (g *gp) addDailyProfit(ctx context.Context) error {
	if g.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid mining amount")
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

	logger.Sugar().Infow(
		"addDailyProfit",
		"goodID", g.goodID,
		"goodName", g.goodName,
		"dailyProfit", g.dailyProfit,
		"userAmount", toUser,
		"platformAmount", toPlatform,
	)

	if toUserD.Cmp(decimal.NewFromInt(0)) > 0 {
		_, err := miningdetailcli.CreateDetail(ctx, &miningdetailpb.DetailReq{
			GoodID:               &g.goodID,
			CoinTypeID:           &g.coinTypeID,
			Amount:               &amount,
			BenefitIntervalHours: &g.benefitIntervalHours,
		})
		if err != nil {
			return err
		}
	}

	if toPlatformD.Cmp(decimal.NewFromInt(0)) > 0 {
		_, err := mininggeneralcli.AddGeneral(ctx, &mininggeneralpb.GeneralReq{
			ID:         &g.miningGeneralID,
			Amount:     &amount,
			ToPlatform: &toPlatform,
			ToUser:     &toUser,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *gp) benefitBalance(ctx context.Context) (decimal.Decimal, error) {
	benefit, err := gbmwcli.GetAccountOnly(ctx, &gbmwpb.Conds{
		GoodID: &commonpb.StringVal{
			Op:    cruder.EQ,
			Value: g.goodID,
		},
		Backup: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Active: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: true,
		},
		Locked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
		Blocked: &commonpb.BoolVal{
			Op:    cruder.EQ,
			Value: false,
		},
	})
	if err != nil {
		return decimal.NewFromInt(0), err
	}
	if benefit == nil {
		return decimal.NewFromInt(0), fmt.Errorf("invalid good benefit")
	}

	g.benefitAccountID = benefit.AccountID
	g.benefitAddress = benefit.Address

	balance, err := sphinxproxycli.GetBalance(ctx, &sphinxproxypb.GetBalanceRequest{
		Name:    g.coinName,
		Address: g.benefitAddress,
	})
	if err != nil {
		return decimal.NewFromInt(0), err
	}

	return decimal.NewFromString(balance.BalanceStr)
}

func (g *gp) processDailyProfit(ctx context.Context) error {
	balance, err := g.benefitBalance(ctx)
	if err != nil {
		return fmt.Errorf("benefit balance error: %v", err)
	}

	logger.Sugar().Infow("processDailyProfit", "goodID", g.goodID, "goodName", g.goodName, "benefitAddress",
		g.benefitAddress, "balance", balance, "reserveAmount", g.coinReservedAmount)

	if balance.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	// TODO: here we should check the last transaction state and created time

	g.dailyProfit = balance.
		Sub(g.coinReservedAmount)

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

func (g *gp) processOrder(ctx context.Context, order *orderpb.Order) error {
	if g.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid profit amount")
	}

	amount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(order.Units))).
		Div(decimal.NewFromInt(int64(g.totalUnits))).
		String()
	// TODO: here we should record profit detail id
	ioExtra := fmt.Sprintf(`{"GoodID": "%v", "OrderID": "%v"}`,
		g.goodID, order.ID)

	// TODO: deduct technique service fee
	// TODO: calculate technique service fee commission

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

func (g *gp) processUnsold(ctx context.Context) error {
	if g.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid profit amount")
	}

	amount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(g.totalUnits - g.serviceUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits))).
		String()

	_, err := miningunsoldcli.CreateUnsold(ctx, &miningunsoldpb.UnsoldReq{
		GoodID:               &g.goodID,
		CoinTypeID:           &g.coinTypeID,
		Amount:               &amount,
		BenefitIntervalHours: &g.benefitIntervalHours,
	})

	return err
}

func (g *gp) transfer(ctx context.Context) error {
	if g.dailyProfit.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid profit amount")
	}

	if g.totalUnits <= 0 {
		return fmt.Errorf("invalid stock units")
	}

	userAmount := g.dailyProfit.
		Mul(decimal.NewFromInt(int64(g.serviceUnits))).
		Div(decimal.NewFromInt(int64(g.totalUnits)))
	platformAmount := g.dailyProfit.
		Sub(userAmount)

	logger.Sugar().Infow(
		"transfer",
		"goodID", g.goodID,
		"goodName", g.goodName,
		"dailyProfit", g.dailyProfit,
		"userAmount", userAmount,
		"platformAmount", platformAmount,
	)

	userAmountS := userAmount.String()
	platformAmountS := platformAmount.String()
	feeAmontS := "0"
	txExtra := fmt.Sprintf(`{"GoodID":"%v","DailyProfit":"%v","UserUnits":%v,"TotalUnits":%v}`,
		g.goodID, g.dailyProfit, g.serviceUnits, g.totalUnits)
	txType := txmgrpb.TxType_TxBenefit

	if userAmount.Cmp(decimal.NewFromInt(0)) > 0 {
		_, err := txmwcli.CreateTx(ctx, &txmgrpb.TxReq{
			CoinTypeID:    &g.coinTypeID,
			FromAccountID: &g.benefitAccountID,
			ToAccountID:   &g.userOnlineAccountID,
			Amount:        &userAmountS,
			FeeAmount:     &feeAmontS,
			Extra:         &txExtra,
			Type:          &txType,
		})
		if err != nil {
			return err
		}
	}

	if platformAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		return nil
	}

	txExtra = fmt.Sprintf(`{"GoodID":"%v","Amount":"%v"}`, g.goodID, platformAmount)

	_, err := txmwcli.CreateTx(ctx, &txmgrpb.TxReq{
		CoinTypeID:    &g.coinTypeID,
		FromAccountID: &g.benefitAccountID,
		ToAccountID:   &g.platformOfflineAccountID,
		Amount:        &platformAmountS,
		FeeAmount:     &feeAmontS,
		Extra:         &txExtra,
		Type:          &txType,
	})
	if err != nil {
		return err
	}

	return nil
}
