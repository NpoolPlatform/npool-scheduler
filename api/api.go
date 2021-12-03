package api

import (
	"context"

	"github.com/NpoolPlatform/cloud-hashing-staker/message/npool"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

type Server struct {
	npool.UnimplementedCloudHashingStakerServer
}

func Register(server grpc.ServiceRegistrar) {
	npool.RegisterCloudHashingStakerServer(server, &Server{})
}

func RegisterGateway(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
	return npool.RegisterCloudHashingStakerHandlerFromEndpoint(context.Background(), mux, endpoint, opts)
}
