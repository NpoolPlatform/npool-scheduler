package version

import (
	"fmt"

	cv "github.com/NpoolPlatform/go-service-framework/pkg/version"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
)

func Version() (*basetypes.VersionResponse, error) {
	info, err := cv.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("get service version error: %w", err)
	}
	return &basetypes.VersionResponse{
		Info: info,
	}, nil
}
