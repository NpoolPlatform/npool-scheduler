package dtm

import (
	"context"
	"time"

	dtmcli "github.com/NpoolPlatform/dtm-cluster/pkg/dtm"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
)

func Do(ctx context.Context, dispose *dtmcli.SagaDispose) error {
	start := time.Now()
	_ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := dtmcli.WithSaga(_ctx, dispose)
	dtmElapsed := time.Since(start)
	logger.Sugar().Warnw(
		"Do",
		"Start", start,
		"DtmElapsed", dtmElapsed,
		"Error", err,
	)
	for _, act := range dispose.Actions {
		logger.Sugar().Warnw(
			"Do",
			"ServiceName", act.ServiceName,
			"Action", act.Action,
			"Revert", act.Revert,
		)
	}
	return err
}
