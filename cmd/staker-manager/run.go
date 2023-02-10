package main

import (
	"github.com/NpoolPlatform/staker-manager/api"
	"github.com/NpoolPlatform/staker-manager/pkg/benefit"
	"github.com/NpoolPlatform/staker-manager/pkg/currency"
	"github.com/NpoolPlatform/staker-manager/pkg/deposit"
	"github.com/NpoolPlatform/staker-manager/pkg/gasfeeder"
	"github.com/NpoolPlatform/staker-manager/pkg/notif/announcement"
	"github.com/NpoolPlatform/staker-manager/pkg/notif/notif"
	"github.com/NpoolPlatform/staker-manager/pkg/order"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/collector"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/limitation"
	"github.com/NpoolPlatform/staker-manager/pkg/sentinel/withdraw"
	"github.com/NpoolPlatform/staker-manager/pkg/transaction"

	apicli "github.com/NpoolPlatform/basal-middleware/pkg/client/api"
	grpc2 "github.com/NpoolPlatform/go-service-framework/pkg/grpc"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	cli "github.com/urfave/cli/v2"

	"google.golang.org/grpc"
)

var runCmd = &cli.Command{
	Name:    "run",
	Aliases: []string{"s"},
	Usage:   "Run the daemon",
	Action: func(c *cli.Context) error {
		go func() {
			if err := grpc2.RunGRPC(rpcRegister); err != nil {
				logger.Sugar().Errorf("fail to run grpc server: %v", err)
			}
		}()

		go transaction.Watch(c.Context)
		go deposit.Watch(c.Context)
		go order.Watch(c.Context)
		go collector.Watch(c.Context)
		go limitation.Watch(c.Context)
		go withdraw.Watch(c.Context)
		go benefit.Watch(c.Context)
		go currency.Watch(c.Context)
		go gasfeeder.Watch(c.Context)
		go notif.Watch(c.Context)
		go announcement.Watch(c.Context)

		return grpc2.RunGRPCGateWay(rpcGatewayRegister)
	},
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
