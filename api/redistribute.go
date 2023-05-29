package api

import (
	"context"

	npool "github.com/NpoolPlatform/message/npool/stakermgr"
	benefit1 "github.com/NpoolPlatform/staker-manager/pkg/benefit"
)

func (s *Server) Redistribute(ctx context.Context, in *npool.RedistributeRequest) (*npool.RedistributeResponse, error) {
	benefit1.Redistribute(in.GetGoodID(), in.GetAmount(), in.GetDateTime())
	return &npool.RedistributeResponse{}, nil
}
