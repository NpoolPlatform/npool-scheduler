package main

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"

	"github.com/NpoolPlatform/staker-manager/api"
	"github.com/NpoolPlatform/staker-manager/pkg/announcement"
	"github.com/NpoolPlatform/staker-manager/pkg/benefit"
	"github.com/NpoolPlatform/staker-manager/pkg/currency"
	"github.com/NpoolPlatform/staker-manager/pkg/deposit"
	"github.com/NpoolPlatform/staker-manager/pkg/gasfeeder"
	"github.com/NpoolPlatform/staker-manager/pkg/notification"
	"github.com/NpoolPlatform/staker-manager/pkg/order"
	"github.com/NpoolPlatform/staker-manager/pkg/pubsub"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/collector"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/limitation"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/withdraw"
	"github.com/NpoolPlatform/staker-manager/pkg/transaction"

	apicli "github.com/NpoolPlatform/basal-middleware/pkg/client/api"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	cli "github.com/urfave/cli/v2"

	"google.golang.org/grpc"
)

var runCmd = &cli.Command{
	Name:    "run",
	Aliases: []string{"s"},
	Usage:   "Run the daemon",
	Action: func(c *cli.Context) error {
		return action.Run(
			c.Context,
			run,
			rpcRegister,
			rpcGatewayRegister,
			watch,
		)
	},
}

func run(ctx context.Context) error {
	return pubsub.Subscribe(ctx)
}

func watch(ctx context.Context) error {
	go transaction.Watch(ctx)
	go deposit.Watch(ctx)
	go order.Watch(ctx)
	go collector.Watch(ctx)
	go limitation.Watch(ctx)
	go withdraw.Watch(ctx)
	go benefit.Watch(ctx)
	go currency.Watch(ctx)
	go gasfeeder.Watch(ctx)
	go notification.Watch(ctx)
	go announcement.Watch(ctx)
	return nil
}

func rpcRegister(server grpc.ServiceRegistrar) error {
	api.Register(server)
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
