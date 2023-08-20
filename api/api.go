package api

import (
	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

type Server struct {
	npool.UnimplementedMiddlewareServer
}

func Register(server grpc.ServiceRegistrar) {
	npool.RegisterMiddlewareServer(server, &Server{})
}

func RegisterGateway(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
	return nil
}
