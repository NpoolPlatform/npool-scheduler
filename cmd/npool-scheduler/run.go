package main

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	"github.com/NpoolPlatform/npool-scheduler/pkg/benefit"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
	"github.com/NpoolPlatform/npool-scheduler/pkg/db"
	"github.com/NpoolPlatform/npool-scheduler/pkg/deposit"
	"github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder"
	"github.com/NpoolPlatform/npool-scheduler/pkg/limitation"
	"github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement"
	// "github.com/NpoolPlatform/npool-scheduler/pkg/order"
	"github.com/NpoolPlatform/npool-scheduler/pkg/pubsub"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue"
	"github.com/NpoolPlatform/npool-scheduler/pkg/withdraw"

	apicli "github.com/NpoolPlatform/basal-middleware/pkg/client/api"
	"github.com/NpoolPlatform/npool-scheduler/api"
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

		benefit.Finalize()
		deposit.Finalize()
		withdraw.Finalize()
		limitation.Finalize()
		txqueue.Finalize()
		announcement.Finalize()
		gasfeeder.Finalize()
		// order.Finalize()

		return err
	},
}

func run(ctx context.Context) error {
	logger.Sugar().Infow(
		"run",
		"Subsystems", config.Subsystems(),
	)
	if err := db.Init(); err != nil {
		return err
	}
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
	// order.Initialize(ctx, cancel)
	gasfeeder.Initialize(ctx, cancel)
	announcement.Initialize(ctx, cancel)
	txqueue.Initialize(ctx, cancel)
	limitation.Initialize(ctx, cancel)
	withdraw.Initialize(ctx, cancel)
	deposit.Initialize(ctx, cancel)
	benefit.Initialize(ctx, cancel)
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

	// _ = apicli.Register(mux) //nolint

	return nil
}
