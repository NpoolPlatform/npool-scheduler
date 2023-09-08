package scheduler

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1"
	config "github.com/NpoolPlatform/npool-scheduler/pkg/config"
	scheduler1 "github.com/NpoolPlatform/npool-scheduler/pkg/scheduler"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) EnableSubsystem(ctx context.Context, in *npool.EnableSubsystemRequest) (*npool.EnableSubsystemResponse, error) {
	for _, info := range in.GetInfos() {
		if b := config.SupportSubsystem(info); b {
			if b {
				return &npool.EnableSubsystemResponse{}, status.Error(codes.InvalidArgument, "already supported")
			}
		}
		config.EnableSubsystem(info)
		scheduler1.InitializeSubsystem(ctx, info)
	}
	subsystems := config.Subsystems()
	return &npool.EnableSubsystemResponse{
		Infos: subsystems,
	}, nil
}
