package sentinel

import (
	"github.com/NpoolPlatform/message/npool/scheduler/mw/v1/sentinel"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

type Server struct {
	sentinel.UnimplementedMiddlewareServer
}

func Register(server grpc.ServiceRegistrar) {
	sentinel.RegisterMiddlewareServer(server, &Server{})
}

func RegisterGateway(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
	return nil
}
