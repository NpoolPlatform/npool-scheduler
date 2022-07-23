package version

import (
	"fmt"

	npool "github.com/NpoolPlatform/message/npool"

	cv "github.com/NpoolPlatform/go-service-framework/pkg/version"
)

func Version() (*npool.VersionResponse, error) {
	info, err := cv.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("get service version error: %w", err)
	}
	return &npool.VersionResponse{
		Info: info,
	}, nil
}
