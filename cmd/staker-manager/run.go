package main

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	"github.com/NpoolPlatform/staker-manager/pkg/deposit"
	"github.com/NpoolPlatform/staker-manager/pkg/goodbenefit"

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

func _watch(ctx context.Context, cancel context.CancelFunc, w func(ctx context.Context)) {
	defer func() {
		if err := recover(); err != nil {
			logger.Sugar().Errorw(
				"Watch",
				"State", "Panic",
				"Error", err,
			)
			cancel()
		}
	}()
	w(ctx)
}

func watch(ctx context.Context, cancel context.CancelFunc) error {
	go shutdown(ctx)
	// go _watch(ctx, cancel, transaction.Watch)
	// go _watch(ctx, cancel, deposit.Watch)
	// go _watch(ctx, cancel, order.Watch)
	// go _watch(ctx, cancel, collector.Watch)
	// go _watch(ctx, cancel, limitation.Watch)
	// go _watch(ctx, cancel, withdraw.Watch)
	// go _watch(ctx, cancel, benefit.Watch)
	// go _watch(ctx, cancel, gasfeeder.Watch)
	// go _watch(ctx, cancel, notification.Watch)
	// go _watch(ctx, cancel, announcement.Watch)
	go _watch(ctx, cancel, goodbenefit.Watch)
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
