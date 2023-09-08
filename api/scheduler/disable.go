package scheduler

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1"
	config "github.com/NpoolPlatform/npool-scheduler/pkg/config"
	scheduler1 "github.com/NpoolPlatform/npool-scheduler/pkg/scheduler"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) DisableSubsystem(ctx context.Context, in *npool.DisableSubsystemRequest) (*npool.DisableSubsystemResponse, error) {
	for _, info := range in.GetInfos() {
		if b := config.SupportSubsystem(info); !b {
			if !b {
				return &npool.DisableSubsystemResponse{}, status.Error(codes.InvalidArgument, "already disabled")
			}
		}
		config.DisableSubsystem(info)
		scheduler1.FinalizeSubsystem(ctx, info)
	}
	subsystems := config.Subsystems()
	return &npool.DisableSubsystemResponse{
		Infos: subsystems,
	}, nil
}
