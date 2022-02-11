package main

import (
	"time"

	"github.com/NpoolPlatform/cloud-hashing-staker/api"
	db "github.com/NpoolPlatform/cloud-hashing-staker/pkg/db"
	msgcli "github.com/NpoolPlatform/cloud-hashing-staker/pkg/message/client"
	msglistener "github.com/NpoolPlatform/cloud-hashing-staker/pkg/message/listener"
	msg "github.com/NpoolPlatform/cloud-hashing-staker/pkg/message/message"
	msgsrv "github.com/NpoolPlatform/cloud-hashing-staker/pkg/message/server"
	paywatcher "github.com/NpoolPlatform/cloud-hashing-staker/pkg/middleware/payment-watcher"

	accounting "github.com/NpoolPlatform/cloud-hashing-staker/pkg/middleware/accounting"

	grpc2 "github.com/NpoolPlatform/go-service-framework/pkg/grpc"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	apimgrcli "github.com/NpoolPlatform/api-manager/pkg/client"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	cli "github.com/urfave/cli/v2"

	"google.golang.org/grpc"
)

var runCmd = &cli.Command{
	Name:    "run",
	Aliases: []string{"s"},
	Usage:   "Run the daemon",
	Action: func(c *cli.Context) error {
		if err := db.Init(); err != nil {
			return err
		}

		go func() {
			if err := grpc2.RunGRPC(rpcRegister); err != nil {
				logger.Sugar().Errorf("fail to run grpc server: %v", err)
			}
		}()

		if err := msgsrv.Init(); err != nil {
			return err
		}
		if err := msgcli.Init(); err != nil {
			return err
		}

		go msglistener.Listen()
		go msgSender()
		go accounting.Run(c.Context)
		go paywatcher.Watch(c.Context)

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

	apimgrcli.Register(mux)

	return nil
}

func msgSender() {
	id := 0
	for {
		err := msgsrv.PublishExample(&msg.Example{
			ID:      id,
			Example: "hello world",
		})
		if err != nil {
			logger.Sugar().Errorf("fail to send example: %v", err)
			return
		}
		id++
		time.Sleep(3 * time.Second)
	}
}
