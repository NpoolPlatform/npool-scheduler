package api

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1"
	config "github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

func (s *Server) ListSubsystems(ctx context.Context, in *npool.ListSubsystemsRequest) (*npool.ListSubsystemsResponse, error) {
	subsystems := config.Subsystems()
	return &npool.ListSubsystemsResponse{
		Infos: subsystems,
	}, nil
}
