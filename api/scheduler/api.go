package scheduler

import (
	scheduler "github.com/NpoolPlatform/message/npool/scheduler/mw/v1"
	"google.golang.org/grpc"
)

type Server struct {
	scheduler.UnimplementedMiddlewareServer
}

func Register(server grpc.ServiceRegistrar) {
	scheduler.RegisterMiddlewareServer(server, &Server{})
}
