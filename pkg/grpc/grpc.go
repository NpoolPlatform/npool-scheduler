package grpc

import (
	"context"
	"time"

	grpc2 "github.com/NpoolPlatform/go-service-framework/pkg/grpc"

	goodsconst "github.com/NpoolPlatform/cloud-hashing-goods/pkg/message/const" //nolint
	goodspb "github.com/NpoolPlatform/message/npool/cloud-hashing-goods"

	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	coininfoconst "github.com/NpoolPlatform/sphinx-coininfo/pkg/message/const" //nolint

	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	sphinxproxyconst "github.com/NpoolPlatform/sphinx-proxy/pkg/message/const" //nolint

	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/message/const" //nolint
	orderpb "github.com/NpoolPlatform/message/npool/cloud-hashing-order"

	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/message/const" //nolint
	billingpb "github.com/NpoolPlatform/message/npool/cloud-hashing-billing"

	appusermgrconst "github.com/NpoolPlatform/appuser-manager/pkg/message/const" //nolint
	appusermgrpb "github.com/NpoolPlatform/message/npool/appusermgr"

	"golang.org/x/xerrors"
)

const (
	grpcTimeout = 60 * time.Second
)

//---------------------------------------------------------------------------------------------------------------------------

func GetGoods(ctx context.Context, in *goodspb.GetGoodsRequest) ([]*goodspb.GoodInfo, error) {
	conn, err := grpc2.GetGRPCConn(goodsconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get goods connection: %v", err)
	}
	defer conn.Close()

	cli := goodspb.NewCloudHashingGoodsClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetGoods(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get goods: %v", err)
	}

	return resp.Infos, nil
}

//---------------------------------------------------------------------------------------------------------------------------

func GetCoinInfos(ctx context.Context, in *coininfopb.GetCoinInfosRequest) ([]*coininfopb.CoinInfo, error) {
	conn, err := grpc2.GetGRPCConn(coininfoconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get coininfo connection: %v", err)
	}
	defer conn.Close()

	cli := coininfopb.NewSphinxCoinInfoClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCoinInfos(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get coin infos: %v", err)
	}

	return resp.Infos, nil
}

func GetCoinInfo(ctx context.Context, in *coininfopb.GetCoinInfoRequest) (*coininfopb.CoinInfo, error) {
	conn, err := grpc2.GetGRPCConn(coininfoconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get coininfo connection: %v", err)
	}
	defer conn.Close()

	cli := coininfopb.NewSphinxCoinInfoClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCoinInfo(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get coin info: %v", err)
	}

	return resp.Info, nil
}

//---------------------------------------------------------------------------------------------------------------------------

func GetOrder(ctx context.Context, in *orderpb.GetOrderRequest) (*orderpb.Order, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}
	defer conn.Close()

	cli := orderpb.NewCloudHashingOrderClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetOrder(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get order: %v", err)
	}

	return resp.Info, nil
}

func GetOrders(ctx context.Context, in *orderpb.GetOrdersRequest) ([]*orderpb.Order, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}
	defer conn.Close()

	cli := orderpb.NewCloudHashingOrderClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetOrders(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get orders: %v", err)
	}

	return resp.Infos, nil
}

func GetOrdersByGood(ctx context.Context, in *orderpb.GetOrdersByGoodRequest) ([]*orderpb.Order, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}
	defer conn.Close()

	cli := orderpb.NewCloudHashingOrderClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetOrdersByGood(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get orders: %v", err)
	}

	return resp.Infos, nil
}

func GetCompensatesByOrder(ctx context.Context, in *orderpb.GetCompensatesByOrderRequest) ([]*orderpb.Compensate, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}
	defer conn.Close()

	cli := orderpb.NewCloudHashingOrderClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCompensatesByOrder(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get compensates: %v", err)
	}

	return resp.Infos, nil
}

func GetPaymentByOrder(ctx context.Context, in *orderpb.GetPaymentByOrderRequest) (*orderpb.Payment, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get payment by order: %v", err)
	}
	defer conn.Close()

	cli := orderpb.NewCloudHashingOrderClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetPaymentByOrder(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get payment: %v", err)
	}

	return resp.Info, nil
}

func UpdatePayment(ctx context.Context, in *orderpb.UpdatePaymentRequest) (*orderpb.Payment, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get payment by order: %v", err)
	}
	defer conn.Close()

	cli := orderpb.NewCloudHashingOrderClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.UpdatePayment(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail update payment: %v", err)
	}

	return resp.Info, nil
}

func GetPaymentsByState(ctx context.Context, in *orderpb.GetPaymentsByStateRequest) ([]*orderpb.Payment, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get payment by order: %v", err)
	}
	defer conn.Close()

	cli := orderpb.NewCloudHashingOrderClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetPaymentsByState(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get payments: %v", err)
	}

	return resp.Infos, nil
}

//---------------------------------------------------------------------------------------------------------------------------

func CreatePlatformBenefit(ctx context.Context, in *billingpb.CreatePlatformBenefitRequest) (*billingpb.PlatformBenefit, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.CreatePlatformBenefit(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail create platform benefit: %v", err)
	}

	return resp.Info, nil
}

func CreateUserBenefit(ctx context.Context, in *billingpb.CreateUserBenefitRequest) (*billingpb.UserBenefit, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.CreateUserBenefit(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail create user benefit: %v", err)
	}

	return resp.Info, nil
}

func GetBillingAccount(ctx context.Context, in *billingpb.GetCoinAccountRequest) (*billingpb.CoinAccountInfo, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCoinAccount(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get coin account: %v", err)
	}

	return resp.Info, nil
}

func GetPlatformSetting(ctx context.Context, in *billingpb.GetPlatformSettingRequest) (*billingpb.PlatformSetting, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetPlatformSetting(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get platform setting: %v", err)
	}

	return resp.Info, nil
}

func GetPlatformBenefitsByGood(ctx context.Context, in *billingpb.GetPlatformBenefitsByGoodRequest) ([]*billingpb.PlatformBenefit, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetPlatformBenefitsByGood(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get platform benefits: %v", err)
	}

	return resp.Infos, nil
}

func GetLatestPlatformBenefitByGood(ctx context.Context, in *billingpb.GetLatestPlatformBenefitByGoodRequest) (*billingpb.PlatformBenefit, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetLatestPlatformBenefitByGood(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get latest platform benefit: %v", err)
	}

	return resp.Info, nil
}

func GetLatestUserBenefitByGoodAppUser(ctx context.Context, in *billingpb.GetLatestUserBenefitByGoodAppUserRequest) (*billingpb.UserBenefit, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetLatestUserBenefitByGoodAppUser(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get latest user benefit: %v", err)
	}

	return resp.Info, nil
}

func GetCoinAccountTransactionsByCoinAccount(ctx context.Context, in *billingpb.GetCoinAccountTransactionsByCoinAccountRequest) ([]*billingpb.CoinAccountTransaction, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCoinAccountTransactionsByCoinAccount(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get coin account transactions: %v", err)
	}

	return resp.Infos, nil
}

func GetCoinAccountTransactionsByState(ctx context.Context, in *billingpb.GetCoinAccountTransactionsByStateRequest) ([]*billingpb.CoinAccountTransaction, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCoinAccountTransactionsByState(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get coin account transactions: %v", err)
	}

	return resp.Infos, nil
}

func GetCoinAccountTransactionsByGoodState(ctx context.Context, in *billingpb.GetCoinAccountTransactionsByGoodStateRequest) ([]*billingpb.CoinAccountTransaction, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCoinAccountTransactionsByGoodState(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get coin account transactions: %v", err)
	}

	return resp.Infos, nil
}

func CreateCoinAccountTransaction(ctx context.Context, in *billingpb.CreateCoinAccountTransactionRequest) (*billingpb.CoinAccountTransaction, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.CreateCoinAccountTransaction(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail create coin account transaction: %v", err)
	}

	return resp.Info, nil
}

func UpdateCoinAccountTransaction(ctx context.Context, in *billingpb.UpdateCoinAccountTransactionRequest) (*billingpb.CoinAccountTransaction, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.UpdateCoinAccountTransaction(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail update coin account transaction: %v", err)
	}

	return resp.Info, nil
}

func GetGoodPaymentByAccount(ctx context.Context, in *billingpb.GetGoodPaymentByAccountRequest) (*billingpb.GoodPayment, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetGoodPaymentByAccount(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get good payment: %v", err)
	}

	return resp.Info, nil
}

func UpdateGoodPayment(ctx context.Context, in *billingpb.UpdateGoodPaymentRequest) (*billingpb.GoodPayment, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.UpdateGoodPayment(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail update good payment: %v", err)
	}

	return resp.Info, nil
}

func GetGoodBenefitByGood(ctx context.Context, in *billingpb.GetGoodBenefitByGoodRequest) (*billingpb.GoodBenefit, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetGoodBenefitByGood(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get good benefit: %v", err)
	}

	return resp.Info, nil
}

func GetCoinSettingByCoin(ctx context.Context, in *billingpb.GetCoinSettingByCoinRequest) (*billingpb.CoinSetting, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetCoinSettingByCoin(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get coin setting: %v", err)
	}

	return resp.Info, nil
}

func GetGoodPayments(ctx context.Context, in *billingpb.GetGoodPaymentsRequest) ([]*billingpb.GoodPayment, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetGoodPayments(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get good payments: %v", err)
	}

	return resp.Infos, nil
}

func CreateUserPaymentBalance(ctx context.Context, in *billingpb.CreateUserPaymentBalanceRequest) (*billingpb.UserPaymentBalance, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get payment by billing: %v", err)
	}
	defer conn.Close()

	cli := billingpb.NewCloudHashingBillingClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.CreateUserPaymentBalance(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail create user payment balance: %v", err)
	}

	return resp.Info, nil
}

//---------------------------------------------------------------------------------------------------------------------------

func CreateAddress(ctx context.Context, in *sphinxproxypb.CreateWalletRequest) (*sphinxproxypb.WalletInfo, error) {
	conn, err := grpc2.GetGRPCConn(sphinxproxyconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get sphinxproxy connection: %v", err)
	}
	defer conn.Close()

	cli := sphinxproxypb.NewSphinxProxyClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.CreateWallet(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail create wallet: %v", err)
	}

	return resp.Info, nil
}

func GetBalance(ctx context.Context, in *sphinxproxypb.GetBalanceRequest) (*sphinxproxypb.BalanceInfo, error) {
	conn, err := grpc2.GetGRPCConn(sphinxproxyconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get sphinxproxy connection: %v", err)
	}
	defer conn.Close()

	cli := sphinxproxypb.NewSphinxProxyClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetBalance(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get balance: %v", err)
	}

	return resp.Info, nil
}

func CreateTransaction(ctx context.Context, in *sphinxproxypb.CreateTransactionRequest) (*sphinxproxypb.TransactionInfo, error) {
	conn, err := grpc2.GetGRPCConn(sphinxproxyconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get sphinxproxy connection: %v", err)
	}
	defer conn.Close()

	cli := sphinxproxypb.NewSphinxProxyClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	_, err = cli.CreateTransaction(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail create transaction: %v", err)
	}

	return nil, nil
}

func GetTransaction(ctx context.Context, in *sphinxproxypb.GetTransactionRequest) (*sphinxproxypb.TransactionInfo, error) {
	conn, err := grpc2.GetGRPCConn(sphinxproxyconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get sphinxproxy connection: %v", err)
	}
	defer conn.Close()

	cli := sphinxproxypb.NewSphinxProxyClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetTransaction(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get transaction: %v", err)
	}

	return resp.Info, nil
}

//---------------------------------------------------------------------------------------------------------------------------

func GetAppUserByAppUser(ctx context.Context, in *appusermgrpb.GetAppUserByAppUserRequest) (*appusermgrpb.AppUser, error) {
	conn, err := grpc2.GetGRPCConn(appusermgrconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get usermgr connection: %v", err)
	}
	defer conn.Close()

	cli := appusermgrpb.NewAppUserManagerClient(conn)

	ctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	resp, err := cli.GetAppUserByAppUser(ctx, in)
	if err != nil {
		return nil, xerrors.Errorf("fail get app user: %v", err)
	}

	return resp.Info, nil
}
