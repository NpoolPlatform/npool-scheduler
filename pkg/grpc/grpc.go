package grpc

import (
	"context"

	grpc2 "github.com/NpoolPlatform/go-service-framework/pkg/grpc"

	goodspb "github.com/NpoolPlatform/cloud-hashing-goods/message/npool"
	goodsconst "github.com/NpoolPlatform/cloud-hashing-goods/pkg/message/const" //nolint

	coininfopb "github.com/NpoolPlatform/message/npool/coininfo"
	coininfoconst "github.com/NpoolPlatform/sphinx-coininfo/pkg/message/const" //nolint

	tradingpb "github.com/NpoolPlatform/message/npool/trading"
	tradingconst "github.com/NpoolPlatform/sphinx-service/pkg/message/const" //nolint

	orderpb "github.com/NpoolPlatform/cloud-hashing-order/message/npool"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/message/const" //nolint

	billingpb "github.com/NpoolPlatform/cloud-hashing-billing/message/npool"
	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/message/const" //nolint

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/emptypb"
)

//---------------------------------------------------------------------------------------------------------------------------

func GetGoods(ctx context.Context, in *goodspb.GetGoodsRequest) (*goodspb.GetGoodsResponse, error) {
	conn, err := grpc2.GetGRPCConn(goodsconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get goods connection: %v", err)
	}

	cli := goodspb.NewCloudHashingGoodsClient(conn)
	return cli.GetGoods(ctx, in)
}

func GetGoodsDetail(ctx context.Context, in *goodspb.GetGoodsDetailRequest) (*goodspb.GetGoodsDetailResponse, error) {
	conn, err := grpc2.GetGRPCConn(goodsconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get goods connection: %v", err)
	}

	cli := goodspb.NewCloudHashingGoodsClient(conn)
	return cli.GetGoodsDetail(ctx, in)
}

func GetGoodDetail(ctx context.Context, in *goodspb.GetGoodDetailRequest) (*goodspb.GetGoodDetailResponse, error) {
	conn, err := grpc2.GetGRPCConn(goodsconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get goods connection: %v", err)
	}

	cli := goodspb.NewCloudHashingGoodsClient(conn)
	return cli.GetGoodDetail(ctx, in)
}

//---------------------------------------------------------------------------------------------------------------------------

func GetCoinInfos(ctx context.Context, in *emptypb.Empty) (*coininfopb.GetCoinInfosResponse, error) {
	conn, err := grpc2.GetGRPCConn(coininfoconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get coininfo connection: %v", err)
	}

	cli := coininfopb.NewSphinxCoinInfoClient(conn)
	return cli.GetCoinInfos(ctx, in)
}

func GetCoinInfo(ctx context.Context, in *coininfopb.GetCoinInfoRequest) (*coininfopb.GetCoinInfoResponse, error) {
	conn, err := grpc2.GetGRPCConn(coininfoconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get coininfo connection: %v", err)
	}

	cli := coininfopb.NewSphinxCoinInfoClient(conn)
	return cli.GetCoinInfo(ctx, in)
}

//---------------------------------------------------------------------------------------------------------------------------

func GetOrder(ctx context.Context, in *orderpb.GetOrderRequest) (*orderpb.GetOrderResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.GetOrder(ctx, in)
}

func GetOrderDetail(ctx context.Context, in *orderpb.GetOrderDetailRequest) (*orderpb.GetOrderDetailResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.GetOrderDetail(ctx, in)
}

func GetOrdersDetailByAppUser(ctx context.Context, in *orderpb.GetOrdersDetailByAppUserRequest) (*orderpb.GetOrdersDetailByAppUserResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.GetOrdersDetailByAppUser(ctx, in)
}

func GetOrdersDetailByApp(ctx context.Context, in *orderpb.GetOrdersDetailByAppRequest) (*orderpb.GetOrdersDetailByAppResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.GetOrdersDetailByApp(ctx, in)
}

func GetOrdersDetailByGood(ctx context.Context, in *orderpb.GetOrdersDetailByGoodRequest) (*orderpb.GetOrdersDetailByGoodResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.GetOrdersDetailByGood(ctx, in)
}

func CreateOrder(ctx context.Context, in *orderpb.CreateOrderRequest) (*orderpb.CreateOrderResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.CreateOrder(ctx, in)
}

func CreateGoodPaying(ctx context.Context, in *orderpb.CreateGoodPayingRequest) (*orderpb.CreateGoodPayingResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.CreateGoodPaying(ctx, in)
}

func CreateGasPaying(ctx context.Context, in *orderpb.CreateGasPayingRequest) (*orderpb.CreateGasPayingResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.CreateGasPaying(ctx, in)
}

func CreatePayment(ctx context.Context, in *orderpb.CreatePaymentRequest) (*orderpb.CreatePaymentResponse, error) {
	conn, err := grpc2.GetGRPCConn(orderconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get order connection: %v", err)
	}

	cli := orderpb.NewCloudHashingOrderClient(conn)
	return cli.CreatePayment(ctx, in)
}

//---------------------------------------------------------------------------------------------------------------------------

func CreateBillingAccount(ctx context.Context, in *billingpb.CreateCoinAccountRequest) (*billingpb.CreateCoinAccountResponse, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}

	cli := billingpb.NewCloudHashingBillingClient(conn)
	return cli.CreateCoinAccount(ctx, in)
}

func GetBillingAccount(ctx context.Context, in *billingpb.GetCoinAccountRequest) (*billingpb.GetCoinAccountResponse, error) {
	conn, err := grpc2.GetGRPCConn(billingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get billing connection: %v", err)
	}

	cli := billingpb.NewCloudHashingBillingClient(conn)
	return cli.GetCoinAccount(ctx, in)
}

//---------------------------------------------------------------------------------------------------------------------------

func CreateCoinAddress(ctx context.Context, in *tradingpb.CreateWalletRequest) (*tradingpb.CreateWalletResponse, error) {
	conn, err := grpc2.GetGRPCConn(tradingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get trading connection: %v", err)
	}

	cli := tradingpb.NewTradingClient(conn)
	return cli.CreateWallet(ctx, in)
}

func GetWalletBalance(ctx context.Context, in *tradingpb.GetWalletBalanceRequest) (*tradingpb.GetWalletBalanceResponse, error) {
	conn, err := grpc2.GetGRPCConn(tradingconst.ServiceName, grpc2.GRPCTAG)
	if err != nil {
		return nil, xerrors.Errorf("fail get trading connection: %v", err)
	}

	cli := tradingpb.NewTradingClient(conn)
	return cli.GetWalletBalance(ctx, in)
}
