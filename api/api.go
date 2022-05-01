package api

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/stakermgr"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

type Server struct {
	npool.UnimplementedStakerManagerServer
}

func Register(server grpc.ServiceRegistrar) {
	npool.RegisterStakerManagerServer(server, &Server{})
}

func RegisterGateway(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
	return npool.RegisterStakerManagerHandlerFromEndpoint(context.Background(), mux, endpoint, opts)
}
