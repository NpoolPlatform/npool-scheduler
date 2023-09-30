package sentinel

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/sentinel"
	sentinel1 "github.com/NpoolPlatform/npool-scheduler/pkg/scheduler/sentinel"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Trigger(ctx context.Context, in *npool.TriggerRequest) (*npool.TriggerResponse, error) {
	if err := sentinel1.Trigger(in); err != nil {
		return &npool.TriggerResponse{}, status.Error(codes.Internal, err.Error())
	}
	return &npool.TriggerResponse{}, nil
}
