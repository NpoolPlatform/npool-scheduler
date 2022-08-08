package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	constant "github.com/NpoolPlatform/go-service-framework/pkg/mysql/const"

	archivement "github.com/NpoolPlatform/staker-manager/pkg/archivement"
	commission "github.com/NpoolPlatform/staker-manager/pkg/commission"

	billingcli "github.com/NpoolPlatform/cloud-hashing-billing/pkg/client"
	billingstconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/const"
	billingent "github.com/NpoolPlatform/cloud-hashing-billing/pkg/db/ent"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/message/const"

	orderent "github.com/NpoolPlatform/cloud-hashing-order/pkg/db/ent"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/message/const"
	orderstpb "github.com/NpoolPlatform/message/npool/order/mgr/v1/order/state"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	ordermw "github.com/NpoolPlatform/order-middleware/pkg/order"

	withdrawcli "github.com/NpoolPlatform/ledger-manager/pkg/client/withdraw"
	ledgermwcli "github.com/NpoolPlatform/ledger-middleware/pkg/client/ledger"
	ledgerdetailpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/detail"
	withdrawmgrpb "github.com/NpoolPlatform/message/npool/ledger/mgr/v1/ledger/withdraw"

	reviewcli "github.com/NpoolPlatform/review-service/pkg/client"
	reviewconst "github.com/NpoolPlatform/review-service/pkg/const"

	"github.com/NpoolPlatform/go-service-framework/pkg/config"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	_ "github.com/go-sql-driver/mysql" // nolint

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	keyUsername = "username"
	keyPassword = "password"
	keyDBName   = "database_name"
)

func dsn(hostname string) (string, error) {
	username := config.GetStringValueWithNameSpace(constant.MysqlServiceName, keyUsername)
	password := config.GetStringValueWithNameSpace(constant.MysqlServiceName, keyPassword)
	dbname := config.GetStringValueWithNameSpace(hostname, keyDBName)

	svc, err := config.PeekService(constant.MysqlServiceName)
	if err != nil {
		logger.Sugar().Warnw("dsb", "error", err)
		return "", err
	}

	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true&interpolateParams=true",
		username, password,
		svc.Address,
		svc.Port,
		dbname,
	), nil
}

func open(hostname string) (conn *sql.DB, err error) {
	hdsn, err := dsn(hostname)
	if err != nil {
		return nil, err
	}

	conn, err = sql.Open("mysql", hdsn)
	if err != nil {
		return nil, err
	}

	// https://github.com/go-sql-driver/mysql
	// See "Important settings" section.
	conn.SetConnMaxLifetime(time.Minute * 3)
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(10)

	return conn, nil
}

func processOrder(ctx context.Context, order *ordermwpb.Order) error {
	// Migrate payments to ledger details and general
	switch order.PaymentState {
	case orderstpb.EState_Paid.String():
	case orderstpb.EState_PaymentTimeout.String():
	case orderstpb.EState_Canceled.String():
	default:
		return nil
	}

	if order.PaymentStartAmount == "" || order.PaymentFinishAmount == "" {
		return nil
	}

	startAmount, err := decimal.NewFromString(order.PaymentStartAmount)
	if err != nil {
		return err
	}

	finishAmount, err := decimal.NewFromString(order.PaymentFinishAmount)
	if err != nil {
		return err
	}

	if finishAmount.Cmp(startAmount) <= 0 {
		return nil
	}

	defer func() {
		logger.Sugar().Infow(
			"processOrder",
			"AppID", order.AppID,
			"UserID", order.UserID,
			"PaymentCoinTypeID", order.PaymentCoinTypeID,
			"OrderID", order.ID,
			"PaymentID", order.PaymentID,
			"Amount", order.PaymentAmount,
			"StartAmount", order.PaymentStartAmount,
			"FinishAmount", order.PaymentFinishAmount,
			"State", order.PaymentState,
			"Error", err,
		)
	}()

	ioExtra := fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, order.PaymentID, order.ID)
	ioType := ledgerdetailpb.IOType_Incoming
	ioSubType := ledgerdetailpb.IOSubType_Payment
	amount := finishAmount.Sub(startAmount).String()

	if err := ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	}); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			logger.Sugar().Warnw("processOrder", "error", err)
		}
	}

	switch order.PaymentState {
	case orderstpb.EState_Paid.String():
	default:
		return nil
	}

	paymentAmount := decimal.RequireFromString(order.PaymentAmount)

	ioExtra = fmt.Sprintf(`{"PaymentID": "%v", "OrderID": "%v"}`, order.PaymentID, order.ID)
	amount = paymentAmount.String()
	ioType = ledgerdetailpb.IOType_Outcoming
	ioSubType = ledgerdetailpb.IOSubType_Payment

	if err := ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
		AppID:      &order.AppID,
		UserID:     &order.UserID,
		CoinTypeID: &order.PaymentCoinTypeID,
		IOType:     &ioType,
		IOSubType:  &ioSubType,
		Amount:     &amount,
		IOExtra:    &ioExtra,
	}); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			logger.Sugar().Warnw("processOrder", "error", err)
		}
	}

	if err := archivement.CalculateArchivement(ctx, order.ID); err != nil {
		logger.Sugar().Warnw("processOrder", "error", err)
	}

	if err := commission.CalculateCommission(ctx, order.ID, true); err != nil {
		logger.Sugar().Warnw("processOrder", "error", err)
	}

	return nil
}

func processOrders(ctx context.Context, order *orderent.Client) error {
	offset := 0
	limit := 1000

	for {
		infos := []*ordermwpb.Order{}

		stm := order.Order.Query().Offset(offset).Limit(limit)
		err := ordermw.Join(stm).Scan(ctx, &infos)
		if err != nil {
			return err
		}
		if len(infos) == 0 {
			return nil
		}

		invalidID := uuid.UUID{}.String()
		for _, info := range infos {
			if info.PaymentID == "" || info.PaymentID == invalidID {
				continue
			}
			if err := processOrder(ctx, ordermw.Post(info)); err != nil {
				logger.Sugar().Warnw("_migrate", "OrderID", info.ID, "PaymentID", info.PaymentID, "error", err)
			}
		}

		offset += limit
	}
}

func processWithdraw(ctx context.Context, withdraw *billingent.UserWithdrawItem) error {
	tx, err := billingcli.GetTransaction(ctx, withdraw.PlatformTransactionID.String())
	if err != nil {
		return err
	}

	state := withdrawmgrpb.WithdrawState_Successful

	if tx != nil {
		switch tx.State {
		case billingstconst.CoinTransactionStateFail:
			state = withdrawmgrpb.WithdrawState_TransactionFail
		case billingstconst.CoinTransactionStateSuccessful:
			state = withdrawmgrpb.WithdrawState_Successful
		default:
			logger.Sugar().Warnw("processWithdraw", "State", tx.State)
			return nil
		}
	} else {
		rvs, err := reviewcli.GetObjectReviews(
			ctx,
			withdraw.AppID.String(),
			billingconst.ServiceName,
			"withdraw",
			withdraw.ID.String(),
		)
		if err != nil {
			return err
		}
		if len(rvs) == 0 {
			return fmt.Errorf("invalid withdraw review")
		}

		for _, rv := range rvs {
			if rv.State == reviewconst.StateRejected {
				state = withdrawmgrpb.WithdrawState_Rejected
				break
			}
		}

		if state != withdrawmgrpb.WithdrawState_Rejected {
			return fmt.Errorf("invalid withdraw state %v", state)
		}
	}

	amount := decimal.NewFromFloat(tx.Amount).String()

	// TODO: move to TX

	id := withdraw.ID.String()
	appID := withdraw.AppID.String()
	userID := withdraw.UserID.String()
	coinTypeID := withdraw.CoinTypeID.String()
	accountID := withdraw.WithdrawToAccountID.String()

	logger.Sugar().Infow(
		"processWithdraw",
		"ID", id,
		"AppID", appID,
		"UserID", userID,
		"CoinTypeID", coinTypeID,
		"AccountID", accountID,
		"Amount", amount,
		"TransactionID", withdraw.PlatformTransactionID,
		"CID", tx.ChainTransactionID,
	)

	_, err = withdrawcli.CreateWithdraw(ctx, &withdrawmgrpb.WithdrawReq{
		ID:         &id,
		AppID:      &appID,
		UserID:     &userID,
		CoinTypeID: &coinTypeID,
		AccountID:  &accountID,
		Amount:     &amount,
	})
	if err != nil {
		return err
	}

	if state == withdrawmgrpb.WithdrawState_Successful {
		ioExtra := fmt.Sprintf(
			`{"WithdrawID":"%v","TransactionID":"%v","CID":"%v","TransactionFee":"%v","AccountID":"%v"}`,
			withdraw.ID,
			withdraw.PlatformTransactionID,
			tx.ChainTransactionID,
			tx.TransactionFee,
			withdraw.WithdrawToAccountID,
		)
		amount := fmt.Sprintf("%v", withdraw.Amount)
		ioType := ledgerdetailpb.IOType_Outcoming
		ioSubType := ledgerdetailpb.IOSubType_Withdrawal

		if err := ledgermwcli.BookKeeping(ctx, &ledgerdetailpb.DetailReq{
			AppID:      &appID,
			UserID:     &userID,
			CoinTypeID: &coinTypeID,
			IOType:     &ioType,
			IOSubType:  &ioSubType,
			Amount:     &amount,
			IOExtra:    &ioExtra,
		}); err != nil {
			return err
		}
	}

	u := &withdrawmgrpb.WithdrawReq{
		ID:                 &id,
		State:              &state,
		ChainTransactionID: &tx.ChainTransactionID,
	}
	_, err = withdrawcli.UpdateWithdraw(ctx, u)
	return err
}

func processWithdraws(ctx context.Context, billing *billingent.Client) error {
	offset := 0
	limit := 1000

	for {
		infos, err := billing.
			UserWithdrawItem.
			Query().
			Offset(offset).
			Limit(limit).
			All(ctx)
		if err != nil {
			return err
		}

		if len(infos) == 0 {
			return nil
		}

		invalidID := uuid.UUID{}
		for _, info := range infos {
			if info.PlatformTransactionID == invalidID {
				continue
			}

			if err := processWithdraw(ctx, info); err != nil {
				logger.Sugar().Errorw("processWithdraws", "ID", info.ID, "TID", info.PlatformTransactionID, "error", err)
			}
		}

		offset += limit
	}
}

func _migrate(ctx context.Context, order *orderent.Client, billing *billingent.Client) error {
	_ = processOrders(ctx, order)      //nolint
	_ = processWithdraws(ctx, billing) //nolint
	return nil
}

func migrate(ctx context.Context, order, billing *sql.DB) error {
	return _migrate(
		ctx,
		orderent.NewClient(
			orderent.Driver(
				entsql.OpenDB(dialect.MySQL, order),
			),
		),
		billingent.NewClient(
			billingent.Driver(
				entsql.OpenDB(dialect.MySQL, billing),
			),
		),
	)
}

func Migrate(ctx context.Context) (err error) {
	logger.Sugar().Infow("Migrate", "Start", "...")
	defer func() {
		logger.Sugar().Infow("Migrate", "Done", "...", "error", err)
	}()

	// Prepare mysql instance for order / billing / ledger
	order, err := open(orderconst.ServiceName)
	if err != nil {
		return err
	}

	billing, err := open(billingconst.ServiceName)
	if err != nil {
		return err
	}

	return migrate(ctx, order, billing)
}
