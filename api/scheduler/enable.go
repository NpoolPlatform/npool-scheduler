package scheduler

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1"
	config "github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

func (s *Server) EnableSubsystem(ctx context.Context, in *npool.EnableSubsystemRequest) (*npool.EnableSubsystemResponse, error) {
	for _, info := range in.GetInfos() {
		config.EnableSubsystem(info)
	}
	subsystems := config.Subsystems()
	return &npool.EnableSubsystemResponse{
		Infos: subsystems,
	}, nil
}
