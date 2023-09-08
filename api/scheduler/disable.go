package scheduler

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1"
	config "github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

func (s *Server) DisableSubsystem(ctx context.Context, in *npool.DisableSubsystemRequest) (*npool.DisableSubsystemResponse, error) {
	for _, info := range in.GetInfos() {
		config.DisableSubsystem(info)
	}
	subsystems := config.Subsystems()
	return &npool.DisableSubsystemResponse{
		Infos: subsystems,
	}, nil
}
