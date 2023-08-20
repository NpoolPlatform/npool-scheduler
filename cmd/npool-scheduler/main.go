package main

import (
	"fmt"
	"os"

	"github.com/NpoolPlatform/go-service-framework/pkg/app"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	"github.com/NpoolPlatform/npool-scheduler/pkg/servicename"

	mysqlconst "github.com/NpoolPlatform/go-service-framework/pkg/mysql/const"
	rabbitmqconst "github.com/NpoolPlatform/go-service-framework/pkg/rabbitmq/const"
	redisconst "github.com/NpoolPlatform/go-service-framework/pkg/redis/const"

	chainconst "github.com/NpoolPlatform/chain-middleware/pkg/servicename"
	ledgerconst "github.com/NpoolPlatform/ledger-manager/pkg/message/const"
	orderconst "github.com/NpoolPlatform/order-middleware/pkg/message/const"
	thirdconst "github.com/NpoolPlatform/third-middleware/pkg/message/const"

	cli "github.com/urfave/cli/v2"
)

const serviceName = servicename.ServiceName

func main() {
	commands := cli.Commands{
		runCmd,
	}

	description := fmt.Sprintf("my %v service cli\nFor help on any individual command run <%v COMMAND -h>\n",
		serviceName, serviceName)
	err := app.Init(
		serviceName,
		description,
		"",
		"",
		"./",
		nil,
		commands,
		redisconst.RedisServiceName,
		mysqlconst.MysqlServiceName,
		rabbitmqconst.RabbitMQServiceName,
		ledgerconst.ServiceName,
		orderconst.ServiceName,
		chainconst.ServiceDomain,
		thirdconst.ServiceName,
	)
	if err != nil {
		logger.Sugar().Errorf("fail to create %v: %v", serviceName, err)
		return
	}
	err = app.Run(os.Args)
	if err != nil {
		logger.Sugar().Errorf("fail to run %v: %v", serviceName, err)
	}
}
