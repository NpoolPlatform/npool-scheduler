package benefit

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	orderpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"

	miningdetailcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/mining/detail"
	mininggeneralcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/mining/general"
	miningunsoldcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/mining/unsold"
	mininggeneralpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/mining/general"
	miningdetailpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/mining/detail"
	miningunsoldpb "github.com/NpoolPlatform/message/npool/ledger/mw/v2/mining/unsold"

	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"

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
