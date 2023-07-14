package main

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	"github.com/NpoolPlatform/staker-manager/pkg/announcement"
	"github.com/NpoolPlatform/staker-manager/pkg/benefit"
	"github.com/NpoolPlatform/staker-manager/pkg/deposit"
	"github.com/NpoolPlatform/staker-manager/pkg/gasfeeder"
	"github.com/NpoolPlatform/staker-manager/pkg/goodbenefit"
	goodbenefit1 "github.com/NpoolPlatform/staker-manager/pkg/goodbenefit/ticker"
	"github.com/NpoolPlatform/staker-manager/pkg/notification"
	"github.com/NpoolPlatform/staker-manager/pkg/order"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/collector"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/limitation"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/withdraw"
	"github.com/NpoolPlatform/staker-manager/pkg/transaction"

	"github.com/NpoolPlatform/staker-manager/pkg/pubsub"

	apicli "github.com/NpoolPlatform/basal-middleware/pkg/client/api"
	"github.com/NpoolPlatform/staker-manager/api"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	cli "github.com/urfave/cli/v2"

	"google.golang.org/grpc"
)

var runCmd = &cli.Command{
	Name:    "run",
	Aliases: []string{"s"},
	Usage:   "Run the daemon",
	Action: func(c *cli.Context) error {
		err := action.Run(
			c.Context,
			run,
			rpcRegister,
			rpcGatewayRegister,
			watch,
		)

		deposit.Shutdown()

		return err
	},
}

func run(ctx context.Context) error {
	return pubsub.Subscribe(ctx)
}

func shutdown(ctx context.Context) {
	<-ctx.Done()
	logger.Sugar().Infow(
		"Watch",
		"State", "Done",
		"Error", ctx.Err(),
	)
	_ = pubsub.Shutdown(ctx) //nolint
}

func watch(ctx context.Context, cancel context.CancelFunc) error {
	go shutdown(ctx)
	go action.Watch(ctx, cancel, transaction.Watch)
	go action.Watch(ctx, cancel, deposit.Watch)
	go action.Watch(ctx, cancel, order.Watch)
	go action.Watch(ctx, cancel, collector.Watch)
	go action.Watch(ctx, cancel, limitation.Watch)
	go action.Watch(ctx, cancel, withdraw.Watch)
	go action.Watch(ctx, cancel, benefit.Watch)
	go action.Watch(ctx, cancel, gasfeeder.Watch)
	go action.Watch(ctx, cancel, notification.Watch)
	go action.Watch(ctx, cancel, announcement.Watch)
	go action.Watch(ctx, cancel, goodbenefit.Watch)
	go action.Watch(ctx, cancel, goodbenefit1.Watch)
	return nil
}

func rpcRegister(server grpc.ServiceRegistrar) error {
	api.Register(server)
	apicli.RegisterGRPC(server)
	return nil
}

func rpcGatewayRegister(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
	err := api.RegisterGateway(mux, endpoint, opts)
	if err != nil {
		return err
	}

	_ = apicli.Register(mux) //nolint

	return nil
}
